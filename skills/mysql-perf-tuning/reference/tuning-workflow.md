# MySQL Performance Tuning Workflow

Detailed step-by-step methodology for each phase of MySQL performance tuning using the mysqltuner_mcp MCP server.

---

## Table of Contents

1. [Phase 1: Prerequisites and Health Check](#phase-1-prerequisites-and-health-check)
2. [Phase 2: Query Optimization Workflow](#phase-2-query-optimization-workflow)
3. [Phase 3: Index Optimization](#phase-3-index-optimization)
4. [Phase 4: InnoDB Deep-Dive](#phase-4-innodb-deep-dive)
5. [Phase 5: Connection and Lock Analysis](#phase-5-connection-and-lock-analysis)
6. [Phase 6: Storage and Capacity Planning](#phase-6-storage-and-capacity-planning)
7. [Phase 7: Security Review](#phase-7-security-review)

---

## Phase 1: Prerequisites and Health Check

### Step 1.1: Verify Performance Schema

```
Tool: check_perf_schema_config
```

Check the `tool_readiness` field in the response. If key instruments are disabled:
- Statement instruments OFF → `get_slow_queries` and statement analysis tools will return no data
- Wait instruments OFF → `analyze_wait_events` will return no data
- Memory instruments OFF → `get_memory_by_host` will be limited

If `performance_schema` itself is OFF, communicate to the user that many diagnostic tools will not work and recommend enabling it (requires MySQL restart).

### Step 1.2: Overall Health Assessment

```
Tool: check_database_health
```

The health score breakdown shows which areas are contributing to deductions:
- Connection usage >70% → investigate connection pooling
- Buffer pool hit ratio <95% → investigate memory allocation
- Slow query % >0.1% → investigate query optimization
- Table scan ratio high → investigate missing indexes
- Disk temp table % >25% → investigate temp table sizing or query patterns

### Step 1.3: Throughput Baseline

```
Tool: get_global_status_snapshot (category: "all")
```

Record key metrics for baseline comparison:
- **QPS** (Questions per second) - overall throughput
- **Read/Write ratio** - workload characterization (OLTP is typically 60-80% reads)
- **Slow query percentage** - quality indicator
- **Bytes sent/received** - network throughput

### Step 1.4: Configuration Overview

```
Tool: review_settings
```

```
Tool: calculate_memory_usage
```

Check whether MySQL's configured maximum memory exceeds physical RAM. This is a common misconfiguration that can cause OOM kills.

---

## Phase 2: Query Optimization Workflow

This is the highest-impact area. A systematic approach to finding and fixing bad queries.

### Step 2.1: Find the Worst Queries

```
Tool: get_slow_queries (order_by: "total_time", limit: 20)
```

This returns queries from `performance_schema.events_statements_summary_by_digest` ranked by total execution time. Focus on the top 5 - these are your biggest wins.

For each query, note:
- **Total time** - Cumulative wall-clock time across all executions
- **Avg time** - Per-execution time (high avg = expensive query)
- **Call count** - How often it runs (high count * even moderate avg = big total)
- **Rows examined vs rows sent** - Ratio >10:1 indicates missing or poor indexes

### Step 2.2: Identify Problematic Query Patterns

Run these tools in parallel to find queries with specific anti-patterns:

```
Tool: get_statements_with_full_scans    → Queries doing full table scans
Tool: get_statements_with_temp_tables   → Queries spilling temp tables to disk
Tool: get_statements_with_sorting       → Queries with expensive filesort
Tool: get_statements_with_errors        → Queries generating errors/warnings
```

### Step 2.3: Analyze Individual Queries

For each problematic query identified above:

```
Tool: analyze_query (query: "<the SQL>", format: "json")
```

JSON format gives the most detail. Look for these red flags in the EXPLAIN output:
- `access_type: "ALL"` → Full table scan (most expensive)
- `access_type: "index"` → Full index scan (less bad but still costly)
- `using_filesort: true` → Needs sort buffer, may spill to disk
- `using_temporary: true` → Creates temporary table
- `possible_keys` is populated but `key` is NULL → Optimizer chose not to use available indexes
- `rows_examined` >> `rows_produced` → Reading far more rows than needed

### Step 2.4: Check for Implicit Type Conversions

```
Tool: analyze_long_queries_for_type_collation_issues
```

Implicit type conversions (comparing VARCHAR to INT, mismatched collations) silently prevent index usage. This is one of the most common hidden performance killers.

### Step 2.5: Generate Index Recommendations

```
Tool: get_index_recommendations
```

This analyzes query patterns and generates `CREATE INDEX` DDL statements. Review each recommendation:
- Does the table already have many indexes? (>6-8 indexes may slow writes)
- Is the suggested index a prefix of an existing composite index?
- Will the query actually benefit (check with EXPLAIN after adding the index)?

---

## Phase 3: Index Optimization

### Step 3.1: Remove Waste First

```
Tool: find_unused_indexes
```

This tool finds three categories:
1. **Unused indexes** - Zero reads since last server restart. Safe to drop after confirming they aren't used for special queries (monthly reports, etc.)
2. **Duplicate indexes** - Same columns in the same order. Always safe to drop the duplicate.
3. **Redundant indexes** - One index is a prefix of another (e.g., `idx_a` on `(a)` is redundant if `idx_a_b` on `(a, b)` exists). The prefix index can be dropped.

Generate `DROP INDEX` statements for confirmed waste.

### Step 3.2: Add Missing Indexes

```
Tool: get_index_recommendations
```

After removing unused indexes, add indexes suggested by query pattern analysis.

### Step 3.3: Verify Index Quality

```
Tool: get_index_stats (schema: "<target_schema>")
```

Check:
- **Selectivity** - Unique values / total rows. Below 0.1 = low selectivity, index may not help much
- **Read/write ratio** - An index with high writes but zero reads is waste
- **Cardinality** - Low cardinality columns (e.g., boolean, enum) are poor index candidates alone

---

## Phase 4: InnoDB Deep-Dive

### Step 4.1: Buffer Pool Analysis

```
Tool: analyze_buffer_pool
```

Key metrics:
- **Hit ratio** - Should be >99% for OLTP. If <95%, the buffer pool is too small.
- **Pages free** - If consistently near 0, the buffer pool is under pressure.
- **Pages dirty** - High dirty page count indicates write pressure or slow flushing.
- **Per-table breakdown** - Shows which tables consume the most buffer pool space.

Formula for sizing: `innodb_buffer_pool_size` should be 70-80% of available RAM on a dedicated MySQL server, but never more than the total dataset size.

### Step 4.2: InnoDB Status Analysis

```
Tool: get_innodb_status
```

Parse the output for:
- **History list length** - Above 1000 indicates purge lag. Long-running transactions may prevent undo log cleanup.
- **Log sequence number vs last checkpoint** - Large gap means redo log is filling up. Consider increasing `innodb_log_file_size`.
- **Deadlocks** - The latest deadlock info shows the conflicting queries and tables.

### Step 4.3: Transaction Analysis

```
Tool: analyze_innodb_transactions
```

Look for:
- Transactions running for more than a few seconds in OLTP workloads
- Transactions in LOCK WAIT state
- High trx_rows_locked counts indicating lock escalation

### Step 4.4: Wait Event Analysis

```
Tool: analyze_wait_events
```

The wait events reveal the true bottleneck:
- **I/O waits dominate** → Disk is the bottleneck. Increase buffer pool, use faster storage, or reduce I/O-heavy queries.
- **Lock waits dominate** → Concurrency issue. Find long-running transactions, optimize locking strategy.
- **Buffer waits** → Buffer pool contention. Increase `innodb_buffer_pool_instances`.
- **Log waits** → Redo log is a bottleneck. Increase `innodb_log_file_size`, tune `innodb_flush_log_at_trx_commit`.

---

## Phase 5: Connection and Lock Analysis

### Step 5.1: Connection State Breakdown

```
Tool: analyze_connections (group_by: "state")
Tool: analyze_connections (group_by: "user")
```

Common findings:
- Majority sleeping → Application not using connection pooling, or `wait_timeout` too high
- Many connections from one user/host → Possible connection leak
- High aborted_connects → Authentication failures or network issues

### Step 5.2: Lock Contention Analysis

```
Tool: analyze_table_locks
```

Check:
- `table_lock_wait_pct` > 1% → MyISAM tables or DDL contention. Convert to InnoDB.
- `innodb_row_lock_time_avg` > 1000ms → Long transactions holding locks
- Metadata locks in PENDING state → DDL waiting for DML transactions to complete

### Step 5.3: Temp Table Analysis

```
Tool: analyze_temp_tables
```

If disk_tmp_pct > 25%:
- Check `tmp_table_size` and `max_heap_table_size` - they should be equal
- Look at top_disk_temp_queries - queries with GROUP BY, DISTINCT, or UNION often create disk temp tables
- Queries with BLOB/TEXT columns always create disk temp tables (can't use MEMORY engine)

---

## Phase 6: Storage and Capacity Planning

### Step 6.1: Schema Size Profiling

```
Tool: profile_schema_sizes
```

Identify:
- The largest databases and tables
- Tables with high free_space (candidates for `OPTIMIZE TABLE`)
- Index-to-data ratio anomalies (>1.5x suggests over-indexing)

### Step 6.2: Fragmentation

```
Tool: get_fragmented_tables
```

Tables with fragmentation > 20% and significant size (>100MB) are candidates for `OPTIMIZE TABLE`. Note: `OPTIMIZE TABLE` on InnoDB performs a full table rebuild and is an online DDL operation in MySQL 5.6+, but it will still use I/O and CPU.

### Step 6.3: Auto-Increment Headroom

```
Tool: analyze_auto_increment (threshold: 75)
```

Tables approaching their auto-increment maximum will cause insert failures. INT columns max out at ~2.1 billion. The fix is `ALTER TABLE ... MODIFY id BIGINT` which requires a table rebuild.

### Step 6.4: Binary Log Analysis

```
Tool: analyze_binlog
```

Check:
- Binlog expiration is set (otherwise old files accumulate)
- `sync_binlog = 1` for durability
- Binlog format is ROW for deterministic replication
- Binlog cache disk usage is low (<10%)

---

## Phase 7: Security Review

```
Tool: analyze_security
Tool: analyze_user_privileges
Tool: check_audit_log
```

Security issues to flag:
- Anonymous users (users with empty username)
- Users without passwords
- Root accessible from remote hosts
- Users with dangerous privileges (SUPER, FILE, PROCESS, SHUTDOWN)
- Wildcard host entries (`%`)
- Test databases accessible to all users
- No password validation plugin
- SSL/TLS not enforced
