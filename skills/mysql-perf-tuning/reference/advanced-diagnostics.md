# Advanced MySQL Diagnostics

Covers diagnostic scenarios and techniques beyond the 39 MCP tools. Use these when standard tool output doesn't fully explain the problem, or for maintenance and operational tasks that require deeper investigation.

---

## Table of Contents

1. [Optimizer Trace Analysis](#optimizer-trace-analysis)
2. [Table Maintenance Operations](#table-maintenance-operations)
3. [Schema Change Impact Assessment](#schema-change-impact-assessment)
4. [Error Log Pattern Analysis](#error-log-pattern-analysis)
5. [Historical Delta Comparison](#historical-delta-comparison)
6. [Security Deep Dive](#security-deep-dive)
7. [Replication Advanced Troubleshooting](#replication-advanced-troubleshooting)
8. [Data Integrity Verification](#data-integrity-verification)
9. [InnoDB Internals Deep Dive](#innodb-internals-deep-dive)

---

## Optimizer Trace Analysis

**When to use**: EXPLAIN shows a bad plan but you can't understand why the optimizer chose it. Use optimizer trace to see the full decision-making process.

### Enable and capture

```sql
-- Session-level only (does not affect other connections)
SET optimizer_trace = 'enabled=on';
SET optimizer_trace_max_mem_size = 1048576;

-- Run the problematic query
SELECT /* trace this */ ... ;

-- Read the full trace
SELECT TRACE FROM information_schema.OPTIMIZER_TRACE\G

-- Disable when done
SET optimizer_trace = 'enabled=off';
```

### What to look for in the trace

| Section | Key Fields | What It Tells You |
|---------|-----------|-------------------|
| `join_preparation` | Rewrites, transformations | How the optimizer simplified the query |
| `rows_estimation` | `table_scan.rows`, `potential_range_indexes` | Why it chose table scan vs index |
| `considered_execution_plans` | `cost`, `chosen` | The plan cost comparison and final choice |
| `attaching_conditions_to_tables` | pushed conditions | Which WHERE conditions were pushed to storage |

### Common insights

- **Index not chosen despite existing**: Check `rows_estimation` — the optimizer may estimate the index covers too many rows (> 30% of table)
- **Subquery materialized instead of correlated**: Check `join_preparation` for subquery transformation decisions
- **Bad join order**: Check `considered_execution_plans` for the cost comparison between different join orders

### Histogram statistics (MySQL 8.0+)

```sql
-- Create histogram on a column (helps optimizer make better row estimates)
ANALYZE TABLE schema_name.table_name UPDATE HISTOGRAM ON column_name WITH 100 BUCKETS;

-- View existing histograms
SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME,
       JSON_EXTRACT(HISTOGRAM, '$.\"number-of-buckets-specified\"') AS buckets,
       JSON_EXTRACT(HISTOGRAM, '$.\"last-updated\"') AS last_updated
FROM information_schema.COLUMN_STATISTICS;

-- Drop a histogram
ANALYZE TABLE schema_name.table_name DROP HISTOGRAM ON column_name;
```

---

## Table Maintenance Operations

### When to run maintenance

| Scenario | Action | Urgency |
|----------|--------|---------|
| After bulk INSERT/DELETE (>20% of rows) | `ANALYZE TABLE` | High — stale stats cause bad plans |
| Fragmentation > 20% | `OPTIMIZE TABLE` or `ALTER TABLE ... ENGINE=InnoDB` | Medium |
| Suspected corruption | `CHECK TABLE` | High |
| After adding/dropping indexes | `ANALYZE TABLE` | Medium — optimizer needs fresh cardinality |
| Regular maintenance (weekly/monthly) | `ANALYZE TABLE` on high-churn tables | Low — preventive |

### ANALYZE TABLE

Updates index statistics used by the optimizer. Safe to run online.

```sql
-- Single table
ANALYZE TABLE schema_name.table_name;

-- Check all tables in a schema
SELECT CONCAT('ANALYZE TABLE `', TABLE_SCHEMA, '`.`', TABLE_NAME, '`;')
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'your_schema'
  AND TABLE_TYPE = 'BASE TABLE';
```

### OPTIMIZE TABLE

Rebuilds the table and reclaims fragmented space. **Requires table lock for the duration** (InnoDB uses online DDL internally but still briefly locks).

```sql
-- Check fragmentation first
SELECT TABLE_NAME,
       ROUND(DATA_FREE / 1024 / 1024, 2) AS free_mb,
       ROUND(DATA_FREE / NULLIF(DATA_LENGTH + INDEX_LENGTH, 0) * 100, 1) AS frag_pct
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'your_schema'
  AND DATA_FREE > 10485760;

-- Optimize high-fragmentation tables
OPTIMIZE TABLE schema_name.table_name;
-- OR (preferred for production — uses online ALTER):
ALTER TABLE schema_name.table_name ENGINE=InnoDB;

-- Always update stats after:
ANALYZE TABLE schema_name.table_name;
```

### CHECK TABLE

Checks table and index integrity. Safe to run but takes time on large tables.

```sql
CHECK TABLE schema_name.table_name;
-- Options: QUICK (fastest), MEDIUM (default), EXTENDED (slowest, most thorough)
CHECK TABLE schema_name.table_name EXTENDED;
```

---

## Schema Change Impact Assessment

Before running DDL (ALTER TABLE, CREATE INDEX, etc.) on production, assess the impact:

### Estimate table size and row count

```sql
SELECT TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH,
       ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS total_mb,
       ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'your_schema'
  AND TABLE_NAME = 'your_table';
```

### Check for blocking metadata locks

```sql
-- Before DDL, check if there are long-running transactions on the target table
SELECT trx_id, trx_state, trx_started,
       TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS age_sec,
       trx_mysql_thread_id
FROM information_schema.innodb_trx
ORDER BY trx_started;

-- Monitor metadata lock queue during DDL
SELECT OBJECT_TYPE, OBJECT_SCHEMA, OBJECT_NAME,
       LOCK_TYPE, LOCK_STATUS, OWNER_THREAD_ID
FROM performance_schema.metadata_locks
WHERE OBJECT_SCHEMA = 'your_schema'
  AND OBJECT_NAME = 'your_table';
```

### Online DDL capability check

| Operation | Online? | Rebuild? | Concurrent DML? |
|-----------|---------|----------|-----------------|
| Add index | Yes | No | Yes |
| Drop index | Yes | No | Yes |
| Add column (last position) | Yes | Yes | Yes |
| Change column type | No | Yes | No (table locked) |
| Add FULLTEXT index | No | Yes | No |
| Convert charset | No | Yes | No |

**Tip**: For risky DDL on large tables, consider tools like `pt-online-schema-change` or `gh-ost` that perform changes without locking.

---

## Error Log Pattern Analysis

MySQL error log contains critical diagnostic information not available through performance_schema.

### Key patterns to search for

| Pattern | Meaning |
|---------|---------|
| `[Warning] Aborted connection` | Client disconnected without proper close (network issue or app bug) |
| `InnoDB: page_cleaner` | Page cleaner can't keep up — storage I/O bottleneck |
| `InnoDB: Difficult to find free blocks in the buffer pool` | Buffer pool too small for workload |
| `InnoDB: Semaphore wait has lasted > 600 seconds` | Serious internal contention — may indicate a bug |
| `InnoDB: Deadlock found when trying to get lock` | Deadlock (also in SHOW ENGINE INNODB STATUS) |
| `The table ... is full` | Temp table size limit hit, or disk full |
| `Too many connections` | max_connections exhausted |
| `InnoDB: OS file reads, writes, fsyncs` | Storage throughput stats in shutdown/status messages |

### Error log location

```sql
SHOW VARIABLES LIKE 'log_error';
-- Common locations:
-- /var/log/mysql/error.log (Linux)
-- /var/log/mysqld.log (RHEL/CentOS)
-- C:\ProgramData\MySQL\MySQL Server 8.0\Data\hostname.err (Windows)
```

---

## Historical Delta Comparison

The MCP tool `get_global_status_snapshot` provides a point-in-time snapshot. For trend analysis, capture and compare two snapshots:

### Quick delta method

```sql
-- Snapshot 1
CREATE TEMPORARY TABLE IF NOT EXISTS _status_t1 AS
SELECT VARIABLE_NAME, CAST(VARIABLE_VALUE AS UNSIGNED) AS val
FROM performance_schema.global_status
WHERE VARIABLE_VALUE REGEXP '^[0-9]+$';

-- ... wait 60 seconds ...

-- Snapshot 2 and compare
SELECT s1.VARIABLE_NAME AS metric,
       s1.val AS t1_value,
       s2.val AS t2_value,
       (s2.val - s1.val) AS delta,
       ROUND((s2.val - s1.val) / 60, 2) AS per_second
FROM _status_t1 s1
JOIN (
    SELECT VARIABLE_NAME, CAST(VARIABLE_VALUE AS UNSIGNED) AS val
    FROM performance_schema.global_status
    WHERE VARIABLE_VALUE REGEXP '^[0-9]+$'
) s2 ON s1.VARIABLE_NAME = s2.VARIABLE_NAME
WHERE s2.val != s1.val
ORDER BY delta DESC
LIMIT 30;
```

### Key metrics to track over time

| Metric | What It Tells You |
|--------|-------------------|
| `Com_select`, `Com_insert`, `Com_update`, `Com_delete` | DML throughput |
| `Queries` | Total query rate |
| `Slow_queries` | Slow query count (should be low) |
| `Innodb_buffer_pool_reads` | Disk reads (should be low relative to read_requests) |
| `Created_tmp_disk_tables` | Temp tables spilling to disk |
| `Handler_read_rnd_next` | Full table scan rows (high = missing indexes) |
| `Threads_created` | Thread creation rate (should be near zero with thread_cache) |
| `Innodb_row_lock_waits` | Lock contention frequency |
| `Aborted_connects` + `Aborted_clients` | Connection issues |

---

## Security Deep Dive

### Beyond basic security audit

The MCP tools `analyze_security` and `analyze_user_privileges` cover basic posture. These queries go deeper:

**Password age / expiration policy**:
```sql
SELECT User, Host,
       password_expired,
       password_last_changed,
       password_lifetime,
       account_locked
FROM mysql.user
WHERE User NOT IN ('mysql.sys', 'mysql.session', 'mysql.infoschema')
ORDER BY password_last_changed ASC;
```

**Users with SUPER or ALL PRIVILEGES**:
```sql
SELECT User, Host, Super_priv, Grant_priv
FROM mysql.user
WHERE Super_priv = 'Y' OR
      (Select_priv = 'Y' AND Insert_priv = 'Y' AND Update_priv = 'Y'
       AND Delete_priv = 'Y' AND Create_priv = 'Y' AND Drop_priv = 'Y')
ORDER BY User;
```

**DEFINER audit** (routines and views that run with creator's privileges):
```sql
-- Routines
SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE,
       DEFINER, SECURITY_TYPE
FROM information_schema.ROUTINES
WHERE ROUTINE_SCHEMA NOT IN ('mysql', 'sys')
ORDER BY SECURITY_TYPE DESC, DEFINER;

-- Views
SELECT TABLE_SCHEMA, TABLE_NAME, DEFINER, SECURITY_TYPE
FROM information_schema.VIEWS
WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
ORDER BY SECURITY_TYPE DESC, DEFINER;

-- Events
SELECT EVENT_SCHEMA, EVENT_NAME, DEFINER, STATUS
FROM information_schema.EVENTS
ORDER BY DEFINER;

-- Triggers
SELECT TRIGGER_SCHEMA, TRIGGER_NAME, DEFINER,
       EVENT_MANIPULATION, EVENT_OBJECT_TABLE
FROM information_schema.TRIGGERS
WHERE TRIGGER_SCHEMA NOT IN ('mysql', 'sys')
ORDER BY DEFINER;
```

**SSL/TLS connection status**:
```sql
-- Check which users are connected with SSL
SELECT USER, HOST, CONNECTION_TYPE
FROM performance_schema.threads t
JOIN performance_schema.session_connect_attrs a ON t.PROCESSLIST_ID = a.PROCESSLIST_ID
WHERE t.TYPE = 'FOREGROUND'
GROUP BY USER, HOST, CONNECTION_TYPE;

-- Simpler check:
SHOW STATUS LIKE 'Ssl_cipher';
SHOW VARIABLES LIKE 'require_secure_transport';
```

---

## Replication Advanced Troubleshooting

### Errant GTID detection and resolution

```sql
-- On replica: check for errant transactions
-- Step 1: Get source's GTID set
--   (from SHOW REPLICA STATUS → Retrieved_Gtid_Set)

-- Step 2: Find errant GTIDs
SELECT GTID_SUBTRACT(@@global.gtid_executed, '<source_gtid_set>') AS errant_gtids;
-- If result is non-empty, those are errant transactions
```

### Parallel replication bottleneck diagnosis

```sql
-- Check worker utilization
SELECT WORKER_ID, SERVICE_STATE,
       LAST_APPLIED_TRANSACTION,
       APPLYING_TRANSACTION,
       LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP,
       LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE
FROM performance_schema.replication_applier_status_by_worker
ORDER BY WORKER_ID;
```

**Common parallel replication issues**:

| Finding | Cause | Fix |
|---------|-------|-----|
| Only 1 worker active | Tables missing primary keys | Add primary keys to all replicated tables |
| Workers idle, lag increasing | Large transactions serialized | Break large transactions into smaller batches |
| LAST_ERROR_NUMBER non-zero | Applier error | Fix the error (duplicate key, missing row, etc.) |

### Multi-source replication channel triage

```sql
-- List all replication channels
SELECT CHANNEL_NAME, SERVICE_STATE
FROM performance_schema.replication_connection_status;

-- Status per channel
SHOW REPLICA STATUS FOR CHANNEL 'channel_name'\G
```

### Delayed replica monitoring

```sql
-- Check if replica is intentionally delayed
SHOW REPLICA STATUS\G
-- Look for: SQL_Delay (configured delay in seconds)
-- And: SQL_Remaining_Delay (remaining delay before applying)
```

---

## Data Integrity Verification

### Table checksum comparison (source vs replica)

```sql
-- On source:
CHECKSUM TABLE schema_name.table_name;

-- On replica:
CHECKSUM TABLE schema_name.table_name;

-- Compare results — if different, data has drifted
```

> For large-scale verification, consider `pt-table-checksum` from Percona Toolkit.

### InnoDB corruption detection

```sql
-- Check specific table
CHECK TABLE schema_name.table_name EXTENDED;

-- If corruption found:
-- 1. Check error log for InnoDB corruption messages
-- 2. Run: SET GLOBAL innodb_force_recovery = 1;  -- (restart required, escalating levels 1-6)
-- 3. Dump and restore from backup if possible
```

---

## InnoDB Internals Deep Dive

### Adaptive Hash Index (AHI) monitoring

```sql
SHOW ENGINE INNODB STATUS\G
-- Section: "INSERT BUFFER AND ADAPTIVE HASH INDEX"
-- Look for: "hash searches/s" vs "non-hash searches/s"
-- If AHI hit rate is low and memory is needed, consider:
SET GLOBAL innodb_adaptive_hash_index = OFF;
```

### Change buffer monitoring

```sql
SHOW ENGINE INNODB STATUS\G
-- Section: "INSERT BUFFER AND ADAPTIVE HASH INDEX"
-- Look for: "Ibuf: size N, free list len N, seg size N"
-- High change buffer size means secondary index updates are deferred
```

### Doublewrite buffer status

```sql
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM performance_schema.global_status
WHERE VARIABLE_NAME LIKE 'Innodb_dblwr%';
-- Innodb_dblwr_writes: number of doublewrite operations
-- Innodb_dblwr_pages_written: pages written via doublewrite
-- High ratio of pages/writes means efficient batching
```

### InnoDB mutex contention

```sql
SHOW ENGINE INNODB MUTEX;
-- Look for high "os_waits" values — indicates contention
-- Common hotspots:
--   buf_pool_mutex: buffer pool contention (increase instances)
--   log_sys_mutex: redo log contention (increase log buffer)
--   trx_sys_mutex: transaction system contention (reduce transaction frequency)
```
