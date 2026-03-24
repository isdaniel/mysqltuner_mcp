# Common MySQL Performance Problems

Symptom-to-solution mapping for the most frequent MySQL performance issues. Each problem includes the exact tool sequence and interpretation guidance.

---

## Table of Contents

1. [The Server Is Slow (General Slowness)](#the-server-is-slow)
2. [Too Many Connections](#too-many-connections)
3. [Lock Contention and Deadlocks](#lock-contention-and-deadlocks)
4. [Replication Lag](#replication-lag)
5. [Disk Space Running Out](#disk-space-running-out)
6. [High CPU Usage](#high-cpu-usage)
7. [Memory Pressure / OOM Kills](#memory-pressure)
8. [Temp Table Disk Spills](#temp-table-disk-spills)
9. [Slow Specific Query](#slow-specific-query)

---

## The Server Is Slow

**Symptoms**: General application slowness, increased response times across all queries, user complaints about "the database being slow."

**Diagnostic sequence**:

```
Step 1: check_database_health
        → Get overall health score and identify which areas are degraded

Step 2: get_slow_queries (order_by: "total_time", limit: 10)
        → Find the queries consuming the most cumulative time

Step 3: analyze_wait_events
        → Identify the bottleneck type:
          - I/O waits dominate → storage bottleneck
          - Lock waits dominate → concurrency issue
          - CPU/query waits → query optimization needed
```

**Branch based on wait event findings**:

- **I/O bottleneck**:
  ```
  analyze_buffer_pool     → Is buffer pool hit ratio <95%?
  review_settings         → Is innodb_buffer_pool_size too small?
  calculate_memory_usage  → Is there room to increase it?
  ```
  Fix: Increase `innodb_buffer_pool_size` to 70-80% of RAM.

- **Lock bottleneck**:
  ```
  analyze_table_locks           → Which tables have lock contention?
  analyze_innodb_transactions   → Are there long-running transactions?
  get_active_queries            → What queries are running right now?
  ```
  Fix: Optimize transactions, add indexes to reduce lock scope, shorten transaction duration.

- **Query bottleneck**:
  ```
  analyze_query (per slow query)    → Get EXPLAIN plans
  get_index_recommendations         → Suggest missing indexes
  get_statements_with_full_scans    → Find queries doing full table scans
  ```
  Fix: Add indexes, rewrite queries, increase buffer pool.

---

## Too Many Connections

**Symptoms**: `ERROR 1040 (HY000): Too many connections`, application connection timeouts, threads_connected approaching max_connections.

**Diagnostic sequence**:

```
Step 1: analyze_connections (group_by: "state")
        → How many connections are sleeping vs active?

Step 2: analyze_connections (group_by: "user")
        → Which application users consume the most connections?

Step 3: calculate_memory_usage
        → What is the per-connection memory cost?

Step 4: review_settings (category: "connections")
        → Current max_connections, wait_timeout, thread_cache_size
```

**Common findings and fixes**:

| Finding | Fix |
|---------|-----|
| 80%+ connections are sleeping | Reduce `wait_timeout` to 300-600. Implement connection pooling in the application. |
| One user/app consumes most connections | That application has a connection leak. Fix the app or add connection pooling. |
| max_connections is hit but memory allows more | Increase `max_connections` but also increase memory budget accordingly. |
| High aborted_connects | Check for authentication failures, network issues, or firewall timeouts. |
| thread_cache_size too low | Set `thread_cache_size = 16-32` to reduce thread creation overhead. |

**Memory formula**: Each connection costs approximately `read_buffer_size + sort_buffer_size + join_buffer_size + thread_stack + binlog_stmt_cache_size ≈ 1-4MB`. So 500 connections = 0.5-2GB of memory just for thread buffers.

---

## Lock Contention and Deadlocks

**Symptoms**: Queries stuck in "Waiting for table metadata lock", "LOCK WAIT" in InnoDB status, deadlock errors, application timeouts on specific tables.

**Diagnostic sequence**:

```
Step 1: analyze_table_locks
        → Table lock wait percentage and metadata lock details

Step 2: analyze_innodb_transactions
        → Find long-running transactions holding locks

Step 3: get_active_queries (include_sleeping: false)
        → See currently blocked/blocking queries

Step 4: get_innodb_status
        → Check LATEST DETECTED DEADLOCK section

Step 5: analyze_connections (group_by: "state")
        → Find connections in "Locked" or idle-in-transaction state
```

**Common findings and fixes**:

| Finding | Fix |
|---------|-----|
| MyISAM table locks | Convert tables to InnoDB for row-level locking |
| Metadata lock pending | A DDL (ALTER TABLE) is waiting for running queries to finish. Kill long-running queries or run DDL during maintenance window. |
| Long-running transaction (>30s) | Application bug - find and fix the uncommitted transaction |
| Deadlock on specific tables | Ensure all transactions access tables in the same order. Add appropriate indexes to reduce lock scope. |
| innodb_lock_wait_timeout too low | Increase to 50-120 seconds for workloads with expected contention |

---

## Replication Lag

**Symptoms**: Replica shows Seconds_Behind_Master > 0, read-after-write inconsistency, `SHOW REPLICA STATUS` shows large Relay_Log_Space.

**Diagnostic sequence**:

```
Step 1: get_replication_status
        → Current lag, I/O thread and SQL thread status

Step 2: get_slow_queries (on the replica)
        → Queries causing lag on the SQL applier thread

Step 3: analyze_binlog
        → Binlog format and throughput rate

Step 4: review_settings (category: "replication")
        → Parallel replication settings, binlog format
```

**Common findings and fixes**:

| Finding | Fix |
|---------|-----|
| Single-threaded SQL applier | Enable parallel replication: `replica_parallel_workers = 4-16`, `replica_parallel_type = LOGICAL_CLOCK` |
| STATEMENT format causing row-level replays | Switch to ROW format: `binlog_format = ROW` |
| Large transactions on master | Break large batch operations into smaller chunks |
| Slow query on replica | Add indexes on replica (if not using the replica for writes) |
| Network bottleneck | Check `Seconds_Behind_Master` vs `relay_log_space` - if relay logs are caught up but SQL thread is behind, it's an applier issue |

---

## Disk Space Running Out

**Symptoms**: "No space left on device" errors, MySQL refusing writes, binlog accumulation.

**Diagnostic sequence**:

```
Step 1: profile_schema_sizes
        → Which databases/tables use the most space?

Step 2: get_fragmented_tables
        → How much space can be reclaimed by OPTIMIZE TABLE?

Step 3: analyze_binlog
        → Are old binary logs accumulating?

Step 4: find_unused_indexes
        → Unused indexes waste disk space
```

**Common findings and fixes**:

| Finding | Fix |
|---------|-----|
| One table dominates space | Archive old data, partition the table, or move to separate tablespace |
| High fragmentation (>20%) | `OPTIMIZE TABLE table_name` to reclaim space |
| Binlog expiration not set | Set `binlog_expire_logs_seconds = 604800` (7 days) |
| Many unused indexes | Drop unused indexes to reclaim space and speed up writes |
| Large undo tablespace | Long-running transactions prevent purge. Kill idle transactions. |

---

## High CPU Usage

**Symptoms**: MySQL process consuming high CPU, `top` shows mysqld at 100%+, slow query response.

**Diagnostic sequence**:

```
Step 1: get_slow_queries (order_by: "calls", limit: 10)
        → High-frequency queries are CPU consumers

Step 2: get_statements_with_full_scans
        → Full table scans are CPU-intensive

Step 3: get_statements_with_sorting
        → Filesort operations consume CPU

Step 4: analyze_statements
        → Overall statement load ranking

Step 5: review_optimizer_config
        → Check if optimizer is doing excessive work
```

**The fix is almost always query optimization**: Add indexes to eliminate full table scans, rewrite queries to avoid filesort, optimize GROUP BY and ORDER BY operations.

---

## Memory Pressure

**Symptoms**: OOM killer terminates MySQL, swap usage high, buffer pool hit ratio declining.

**Diagnostic sequence**:

```
Step 1: calculate_memory_usage
        → Is configured max memory > physical RAM?

Step 2: analyze_buffer_pool
        → Buffer pool size vs actual data size

Step 3: review_settings (category: "memory")
        → Per-thread buffer sizes

Step 4: analyze_connections
        → How many active connections? (each consumes thread memory)
```

**Common findings and fixes**:

| Finding | Fix |
|---------|-----|
| max_memory > physical RAM | Reduce `max_connections` or reduce `innodb_buffer_pool_size` |
| Buffer pool >> dataset size | Reduce buffer pool to dataset size + 20% overhead |
| Per-thread buffers too large | `sort_buffer_size`, `join_buffer_size`, `read_rnd_buffer_size` should be 256K-2M, not larger |
| Too many connections | Reduce `max_connections` to actual peak + 20% headroom |

---

## Temp Table Disk Spills

**Symptoms**: High `Created_tmp_disk_tables`, slow GROUP BY / DISTINCT queries, elevated disk I/O.

**Diagnostic sequence**:

```
Step 1: analyze_temp_tables
        → Disk temp table percentage and top offending queries

Step 2: get_statements_with_temp_tables
        → Specific queries creating disk temp tables

Step 3: review_settings (category: "memory")
        → tmp_table_size and max_heap_table_size values
```

**Key insight**: The effective temp table size limit is `MIN(tmp_table_size, max_heap_table_size)`. Set them to the same value (64M-256M depending on workload).

Queries with BLOB/TEXT columns **always** create on-disk temp tables regardless of these settings. The only fix is to rewrite the query or avoid selecting BLOB/TEXT columns when doing GROUP BY.

---

## Slow Specific Query

**Symptoms**: User identifies a specific SQL query that is slow.

**Diagnostic sequence**:

```
Step 1: analyze_query (query: "<the SQL>", format: "json")
        → Get EXPLAIN plan with full detail

Step 2: get_table_stats (schema: "<schema>", table_name: "<table>")
        → Table size, row count, existing indexes

Step 3: get_index_recommendations
        → Suggested new indexes

Step 4: analyze_long_queries_for_type_collation_issues
        → Check for implicit type conversions
```

**Optimization checklist**:
1. Does the query have a WHERE clause? If so, are the columns indexed?
2. Does the JOIN use indexed columns on both sides?
3. Is ORDER BY using an index, or causing a filesort?
4. Are there implicit type conversions (VARCHAR vs INT comparisons)?
5. Are there functions applied to indexed columns in WHERE? (e.g., `WHERE YEAR(created_at) = 2024` can't use index)
6. Can the query be rewritten to reduce rows examined?
7. Would a covering index eliminate the need to read the table data?
