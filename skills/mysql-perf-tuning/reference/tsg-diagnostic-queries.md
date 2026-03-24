# TSG Diagnostic Queries for MySQL

Diagnostic SQL queries sourced from Supportability wiki TSGs and common DBA practices. Each section maps raw SQL to the corresponding MCP tool where available, and provides standalone SQL for scenarios beyond tool coverage.

---

## Table of Contents

1. [Slow Query Analysis](#slow-query-analysis)
2. [Lock and Deadlock Diagnosis](#lock-and-deadlock-diagnosis)
3. [Connection Analysis](#connection-analysis)
4. [Memory Investigation](#memory-investigation)
5. [InnoDB Deep Dive](#innodb-deep-dive)
6. [Replication Lag Diagnosis](#replication-lag-diagnosis)
7. [Storage and Fragmentation](#storage-and-fragmentation)
8. [Parameter Tuning Matrix](#parameter-tuning-matrix)
9. [Performance Schema Readiness](#performance-schema-readiness)
10. [Raw Diagnostic Queries Beyond MCP Tools](#raw-diagnostic-queries-beyond-mcp-tools)

---

## Slow Query Analysis

### MCP tools available

| Tool | What it does |
|------|-------------|
| `get_slow_queries` | Top slow statement digests by total/avg time, rows examined |
| `analyze_query` | EXPLAIN / EXPLAIN ANALYZE for a specific query |
| `get_statements_with_full_scans` | Queries doing full table scans |
| `get_statements_with_sorting` | Queries with heavy filesort |
| `get_statements_with_temp_tables` | Queries spilling to disk temp tables |
| `analyze_long_queries_for_type_collation_issues` | Implicit type conversion detection |

### Raw SQL for deeper analysis

**Top 10 queries by total latency (performance_schema)**:
```sql
SELECT DIGEST_TEXT,
       COUNT_STAR AS exec_count,
       ROUND(SUM_TIMER_WAIT/1e12, 2) AS total_latency_sec,
       ROUND(AVG_TIMER_WAIT/1e12, 4) AS avg_latency_sec,
       SUM_ROWS_EXAMINED,
       SUM_ROWS_SENT,
       SUM_NO_INDEX_USED + SUM_NO_GOOD_INDEX_USED AS no_index_count
FROM performance_schema.events_statements_summary_by_digest
WHERE SCHEMA_NAME IS NOT NULL
ORDER BY SUM_TIMER_WAIT DESC
LIMIT 10;
```

**Find queries with high rows_examined/rows_sent ratio** (sign of missing index):
```sql
SELECT DIGEST_TEXT,
       COUNT_STAR,
       SUM_ROWS_EXAMINED,
       SUM_ROWS_SENT,
       ROUND(SUM_ROWS_EXAMINED / NULLIF(SUM_ROWS_SENT, 0), 1) AS exam_to_sent_ratio
FROM performance_schema.events_statements_summary_by_digest
WHERE SUM_ROWS_SENT > 0
  AND SUM_ROWS_EXAMINED / SUM_ROWS_SENT > 100
ORDER BY SUM_ROWS_EXAMINED DESC
LIMIT 10;
```
> Ratio > 100 means the query examines 100x more rows than it returns — strong candidate for index improvement.

**Currently running long queries** (> 5 seconds):
```sql
SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE,
       LEFT(INFO, 200) AS query_text
FROM information_schema.PROCESSLIST
WHERE COMMAND != 'Sleep'
  AND TIME > 5
ORDER BY TIME DESC;
```

---

## Lock and Deadlock Diagnosis

### MCP tools available

| Tool | What it does |
|------|-------------|
| `analyze_table_locks` | Table-level and metadata lock contention |
| `analyze_innodb_transactions` | Long-running transactions and lock waits |
| `get_innodb_status` | Full InnoDB status including latest deadlock |
| `get_active_queries` | Blocked/blocking query identification |

### Raw SQL for deeper analysis

**Current InnoDB lock waits** (who blocks whom):
```sql
SELECT
    r.trx_id AS waiting_trx_id,
    r.trx_mysql_thread_id AS waiting_thread,
    r.trx_query AS waiting_query,
    b.trx_id AS blocking_trx_id,
    b.trx_mysql_thread_id AS blocking_thread,
    b.trx_query AS blocking_query,
    b.trx_started AS blocking_since
FROM performance_schema.data_lock_waits w
JOIN information_schema.innodb_trx b ON b.trx_id = w.BLOCKING_ENGINE_TRANSACTION_ID
JOIN information_schema.innodb_trx r ON r.trx_id = w.REQUESTING_ENGINE_TRANSACTION_ID;
```

**Metadata lock holders** (blocking DDL operations):
```sql
SELECT
    OBJECT_TYPE, OBJECT_SCHEMA, OBJECT_NAME,
    LOCK_TYPE, LOCK_DURATION, LOCK_STATUS,
    OWNER_THREAD_ID, OWNER_EVENT_ID
FROM performance_schema.metadata_locks
WHERE OBJECT_TYPE = 'TABLE'
ORDER BY OBJECT_SCHEMA, OBJECT_NAME;
```

**Detect long-running uncommitted transactions** (common lock holders):
```sql
SELECT trx_id, trx_state, trx_started,
       TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS duration_sec,
       trx_rows_locked, trx_rows_modified,
       trx_mysql_thread_id
FROM information_schema.innodb_trx
WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 30
ORDER BY trx_started ASC;
```

**Latest deadlock** (parsed from InnoDB status):
```sql
SHOW ENGINE INNODB STATUS\G
-- Look for: "LATEST DETECTED DEADLOCK" section
-- Key info: which transactions, which rows, which indexes, who was rolled back
```

---

## Connection Analysis

### MCP tools available

| Tool | What it does |
|------|-------------|
| `analyze_connections` | Connection state breakdown, sleeping vs active, by user/host |
| `calculate_memory_usage` | Per-connection memory cost |
| `review_settings` | max_connections, wait_timeout, thread_cache_size |

### Raw SQL for deeper analysis

**Connection state summary**:
```sql
SELECT COMMAND, COUNT(*) AS count,
       GROUP_CONCAT(DISTINCT USER ORDER BY USER) AS users
FROM information_schema.PROCESSLIST
GROUP BY COMMAND
ORDER BY count DESC;
```

**Top connection consumers by user@host**:
```sql
SELECT USER, HOST, COUNT(*) AS conn_count,
       SUM(IF(COMMAND = 'Sleep', 1, 0)) AS sleeping,
       SUM(IF(COMMAND != 'Sleep', 1, 0)) AS active
FROM information_schema.PROCESSLIST
GROUP BY USER, HOST
ORDER BY conn_count DESC
LIMIT 20;
```

**Connection churn rate** (are connections being created too fast?):
```sql
SELECT
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Connections') AS total_connections,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_created') AS threads_created,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_cached') AS threads_cached,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Aborted_connects') AS aborted_connects,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Max_used_connections') AS max_used_connections;
```

---

## Memory Investigation

### MCP tools available

| Tool | What it does |
|------|-------------|
| `calculate_memory_usage` | Global + per-thread memory estimate vs physical RAM |
| `get_memory_by_host` | Memory by host/user from performance_schema |
| `get_table_memory_usage` | Buffer pool footprint by table |
| `analyze_buffer_pool` | Hit ratio, dirty pages, free pages |

### Raw SQL for deeper analysis

**Global memory allocation breakdown**:
```sql
SELECT EVENT_NAME,
       CURRENT_NUMBER_OF_BYTES_USED AS current_bytes,
       HIGH_NUMBER_OF_BYTES_USED AS peak_bytes
FROM performance_schema.memory_summary_global_by_event_name
WHERE CURRENT_NUMBER_OF_BYTES_USED > 1048576  -- > 1MB
ORDER BY CURRENT_NUMBER_OF_BYTES_USED DESC
LIMIT 20;
```
> Requires `performance_schema` memory instruments enabled.

**Per-thread memory (who consumes the most)**:
```sql
SELECT t.THREAD_ID, t.PROCESSLIST_USER, t.PROCESSLIST_HOST,
       SUM(m.CURRENT_NUMBER_OF_BYTES_USED) AS mem_bytes
FROM performance_schema.memory_summary_by_thread_by_event_name m
JOIN performance_schema.threads t ON t.THREAD_ID = m.THREAD_ID
WHERE t.PROCESSLIST_USER IS NOT NULL
GROUP BY t.THREAD_ID, t.PROCESSLIST_USER, t.PROCESSLIST_HOST
ORDER BY mem_bytes DESC
LIMIT 10;
```

**Worst-case memory formula** (verify server won't OOM):
```sql
SELECT
    (@@innodb_buffer_pool_size
     + @@key_buffer_size
     + @@innodb_log_buffer_size
     + @@max_connections * (@@sort_buffer_size + @@join_buffer_size + @@read_buffer_size + @@read_rnd_buffer_size + @@thread_stack + @@binlog_cache_size)
     + @@tmp_table_size
    ) / 1073741824 AS worst_case_memory_gb;
```

---

## InnoDB Deep Dive

### MCP tools available

| Tool | What it does |
|------|-------------|
| `get_innodb_status` | Full SHOW ENGINE INNODB STATUS parse |
| `analyze_buffer_pool` | Buffer pool stats by schema/table |
| `analyze_innodb_transactions` | Transaction age, lock waits, purge lag |

### Raw SQL for deeper analysis

**Buffer pool hit ratio**:
```sql
SELECT
    (1 - (
        (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads')
        /
        NULLIF((SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests'), 0)
    )) * 100 AS buffer_pool_hit_ratio_pct;
```
> Target: > 99% for OLTP, > 95% for mixed workloads.

**InnoDB purge lag** (undo history length):
```sql
SHOW ENGINE INNODB STATUS\G
-- Check: "History list length" — if > 10000, purge is falling behind
-- Also check: "---PURGE---" section for purge thread status
```

**InnoDB redo log checkpoint lag**:
```sql
SHOW ENGINE INNODB STATUS\G
-- Check: "Log sequence number" vs "Last checkpoint at"
-- If the gap approaches total redo log size, increase innodb_log_file_size
```

**Pending I/O operations** (storage bottleneck indicator):
```sql
SELECT
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_data_pending_reads') AS pending_reads,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_data_pending_writes') AS pending_writes,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_data_pending_fsyncs') AS pending_fsyncs,
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_os_log_pending_writes') AS pending_log_writes;
```

---

## Replication Lag Diagnosis

### MCP tools available

| Tool | What it does |
|------|-------------|
| `get_replication_status` | Replica status, lag, I/O/SQL thread health |
| `analyze_binlog` | Binlog format, size, throughput |

### Raw SQL for deeper analysis

**Detailed replica status**:
```sql
SHOW REPLICA STATUS\G
-- Key fields:
--   Seconds_Behind_Source (lag)
--   Relay_Log_Space (queued work)
--   Retrieved_Gtid_Set vs Executed_Gtid_Set (GTID gap)
--   Last_SQL_Error (applier errors)
--   Replica_SQL_Running_State (current activity)
```

**Check parallel replication worker utilization**:
```sql
SELECT WORKER_ID, THREAD_ID,
       SERVICE_STATE,
       LAST_APPLIED_TRANSACTION,
       APPLYING_TRANSACTION,
       LAST_ERROR_MESSAGE
FROM performance_schema.replication_applier_status_by_worker;
```
> If only 1 of N workers is active, there's a parallel replication bottleneck (typically from tables without primary keys).

**Tables without primary keys** (hurt replication performance):
```sql
SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.ENGINE, t.TABLE_ROWS
FROM information_schema.TABLES t
LEFT JOIN information_schema.TABLE_CONSTRAINTS c
  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
  AND t.TABLE_NAME = c.TABLE_NAME
  AND c.CONSTRAINT_TYPE = 'PRIMARY KEY'
WHERE t.TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND t.TABLE_TYPE = 'BASE TABLE'
  AND c.CONSTRAINT_NAME IS NULL
ORDER BY t.TABLE_ROWS DESC;
```

**Errant GTID detection** (replica has transactions not on source):
```sql
-- On replica:
SELECT @@server_uuid AS replica_uuid;
SHOW REPLICA STATUS\G
-- Compare: Retrieved_Gtid_Set vs Executed_Gtid_Set
-- If Executed contains UUIDs not in Retrieved, those are errant transactions
-- Use: SELECT GTID_SUBTRACT(@@global.gtid_executed, '<source_gtid_set>');
```

---

## Storage and Fragmentation

### MCP tools available

| Tool | What it does |
|------|-------------|
| `profile_schema_sizes` | Database and table size ranking |
| `get_fragmented_tables` | Fragmented tables with reclaimable space |
| `analyze_auto_increment` | Auto-increment overflow risk |
| `analyze_binlog` | Binlog disk usage |

### Raw SQL for deeper analysis

**Table fragmentation details**:
```sql
SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE,
       ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
       ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb,
       ROUND(DATA_FREE / 1024 / 1024, 2) AS free_mb,
       ROUND(DATA_FREE / NULLIF(DATA_LENGTH + INDEX_LENGTH, 0) * 100, 1) AS frag_pct
FROM information_schema.TABLES
WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND DATA_FREE > 10485760  -- > 10MB free space
ORDER BY DATA_FREE DESC
LIMIT 20;
```

**Reclaim space** (maintenance window required):
```sql
-- For InnoDB with innodb_file_per_table=ON:
OPTIMIZE TABLE schema_name.table_name;
-- OR for online operation:
ALTER TABLE schema_name.table_name ENGINE=InnoDB;
-- Always run ANALYZE TABLE after to update statistics:
ANALYZE TABLE schema_name.table_name;
```

**Undo tablespace growth** (long transactions prevent purge):
```sql
SELECT NAME, SUBSYSTEM, FILE_NAME,
       ROUND(FILE_SIZE / 1024 / 1024, 2) AS size_mb
FROM information_schema.INNODB_TABLESPACES
WHERE NAME LIKE 'innodb_undo%' OR SPACE_TYPE = 'Undo'
ORDER BY FILE_SIZE DESC;
```

---

## Parameter Tuning Matrix

Symptom-to-parameter mapping from wiki TSGs:

| Symptom | Parameters to Check | Diagnostic Tool | Notes |
|---------|-------------------|-----------------|-------|
| High CPU | `max_connections`, `thread_cache_size` | `review_settings`, `analyze_connections` | Use connection pooling, reduce concurrency |
| Slow queries | `long_query_time`, `slow_query_log` | `get_slow_queries` | Enable slow log first, then analyze |
| Connection issues | `wait_timeout`, `interactive_timeout`, `connect_timeout` | `analyze_connections` | Reduce idle timeouts |
| Memory bottleneck | `innodb_buffer_pool_size`, per-thread buffers | `calculate_memory_usage`, `analyze_buffer_pool` | Size buffer pool to 70-80% RAM on dedicated server |
| Transaction latency | `innodb_purge_threads`, `innodb_log_file_size` | `get_innodb_status` | Tune purge threads and redo log size |
| Lock contention | `innodb_lock_wait_timeout` | `analyze_table_locks`, `analyze_innodb_transactions` | Reduce timeout to detect deadlocks faster |
| Slow index usage | `innodb_stats_on_metadata` | `review_optimizer_config` | Disable if not needed |
| Connection spikes | `max_connections`, `wait_timeout` | `analyze_connections` | Monitor and adjust based on workload |
| Temp table spills | `tmp_table_size`, `max_heap_table_size` | `analyze_temp_tables` | Set both to same value; BLOB/TEXT always spill |
| Sort-heavy queries | `sort_buffer_size`, `max_sort_length` | `get_statements_with_sorting` | Don't over-size; optimize queries first |
| Replication lag | `replica_parallel_workers`, `replica_parallel_type` | `get_replication_status` | Use LOGICAL_CLOCK with 4-16 workers |

---

## Performance Schema Readiness

### MCP tool

| Tool | What it does |
|------|-------------|
| `check_perf_schema_config` | Verifies performance_schema is enabled and instruments are active |

### Raw SQL to enable key instruments

**Check if performance_schema is ON**:
```sql
SHOW VARIABLES LIKE 'performance_schema';
```

**Enable statement instruments** (if not already):
```sql
UPDATE performance_schema.setup_instruments
SET ENABLED = 'YES', TIMED = 'YES'
WHERE NAME LIKE 'statement/%';

UPDATE performance_schema.setup_consumers
SET ENABLED = 'YES'
WHERE NAME LIKE 'events_statements%';
```

**Enable wait instruments**:
```sql
UPDATE performance_schema.setup_instruments
SET ENABLED = 'YES', TIMED = 'YES'
WHERE NAME LIKE 'wait/%';

UPDATE performance_schema.setup_consumers
SET ENABLED = 'YES'
WHERE NAME LIKE 'events_waits%';
```

**Enable memory instruments** (for memory tracking):
```sql
UPDATE performance_schema.setup_instruments
SET ENABLED = 'YES'
WHERE NAME LIKE 'memory/%';
```
> Note: Memory instruments may need a MySQL restart to take full effect.

---

## Raw Diagnostic Queries Beyond MCP Tools

These queries address scenarios not covered by the 39 MCP tools:

### Optimizer Trace (query plan forensics)

```sql
-- Enable for the current session
SET optimizer_trace = 'enabled=on';
SET optimizer_trace_max_mem_size = 1048576;

-- Run your problematic query
SELECT ... ;

-- Read the trace
SELECT * FROM information_schema.OPTIMIZER_TRACE\G

-- Disable when done
SET optimizer_trace = 'enabled=off';
```
> Use when EXPLAIN alone doesn't explain why the optimizer chose a bad plan.

### DEFINER / SQL SECURITY audit

```sql
-- Find routines with risky DEFINER
SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE,
       DEFINER, SECURITY_TYPE
FROM information_schema.ROUTINES
WHERE SECURITY_TYPE = 'DEFINER'
  AND DEFINER NOT IN ('root@localhost', 'mysql.sys@localhost')
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;

-- Find views with DEFINER security
SELECT TABLE_SCHEMA, TABLE_NAME, DEFINER, SECURITY_TYPE
FROM information_schema.VIEWS
WHERE SECURITY_TYPE = 'DEFINER'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

### Event scheduler audit

```sql
SELECT EVENT_SCHEMA, EVENT_NAME, DEFINER,
       STATUS, EVENT_TYPE, INTERVAL_VALUE, INTERVAL_FIELD,
       LAST_EXECUTED, STARTS, ENDS
FROM information_schema.EVENTS
ORDER BY EVENT_SCHEMA, EVENT_NAME;
```

### Character set / collation mismatch finder

```sql
-- Tables with mixed collations (can cause implicit conversions in JOINs)
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
       CHARACTER_SET_NAME, COLLATION_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND CHARACTER_SET_NAME IS NOT NULL
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
```

### Global status delta comparison (manual trending)

```sql
-- Take snapshot 1
CREATE TEMPORARY TABLE status_snap1 AS
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM performance_schema.global_status;

-- Wait N seconds, then take snapshot 2
CREATE TEMPORARY TABLE status_snap2 AS
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM performance_schema.global_status;

-- Compare deltas
SELECT s1.VARIABLE_NAME,
       CAST(s1.VARIABLE_VALUE AS UNSIGNED) AS val_before,
       CAST(s2.VARIABLE_VALUE AS UNSIGNED) AS val_after,
       CAST(s2.VARIABLE_VALUE AS UNSIGNED) - CAST(s1.VARIABLE_VALUE AS UNSIGNED) AS delta
FROM status_snap1 s1
JOIN status_snap2 s2 ON s1.VARIABLE_NAME = s2.VARIABLE_NAME
WHERE s2.VARIABLE_VALUE != s1.VARIABLE_VALUE
  AND s1.VARIABLE_VALUE REGEXP '^[0-9]+$'
ORDER BY delta DESC
LIMIT 30;
```
> The MCP tool `get_global_status_snapshot` provides a single snapshot. Use this pattern for delta analysis over a time window.

### CHECK TABLE / ANALYZE TABLE maintenance

```sql
-- Update index statistics (important after bulk loads)
ANALYZE TABLE schema_name.table_name;

-- Check table integrity
CHECK TABLE schema_name.table_name;

-- Reclaim space from InnoDB fragmentation
-- Option 1: OPTIMIZE (locks table briefly)
OPTIMIZE TABLE schema_name.table_name;
-- Option 2: Online rebuild (preferred for production)
ALTER TABLE schema_name.table_name ENGINE=InnoDB;
```
