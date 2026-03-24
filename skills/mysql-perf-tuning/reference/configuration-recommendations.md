# MySQL Configuration Recommendations

Specific MySQL configuration values and formulas for different workload types. All values include the current-value-vs-recommended comparison format for clear reporting.

---

## Table of Contents

1. [InnoDB Settings](#innodb-settings)
2. [Memory Settings](#memory-settings)
3. [Connection Settings](#connection-settings)
4. [Query and Optimizer Settings](#query-and-optimizer-settings)
5. [Logging Settings](#logging-settings)
6. [Replication Settings](#replication-settings)
7. [Security Settings](#security-settings)
8. [Workload-Specific Profiles](#workload-specific-profiles)

---

## InnoDB Settings

### innodb_buffer_pool_size
The single most impactful setting for InnoDB performance.

| Workload | Formula | Example (32GB RAM) |
|----------|---------|-------------------|
| Dedicated MySQL server | 70-80% of physical RAM | 22-25GB |
| Shared server (with app) | 50-60% of physical RAM | 16-19GB |
| Small/dev instance | Dataset size + 20% | Varies |

**Check with**: `analyze_buffer_pool` → if hit ratio < 99%, increase this value.

**Dynamic**: `SET GLOBAL innodb_buffer_pool_size = <bytes>;` (MySQL 5.7+, takes effect gradually)

### innodb_buffer_pool_instances
Reduces contention on the buffer pool mutex.

| Buffer Pool Size | Recommended Instances |
|-----------------|----------------------|
| < 1GB | 1 |
| 1-8GB | 4-8 |
| 8-32GB | 8-16 |
| > 32GB | 16-32 |

**Rule**: 1 instance per 1-2GB of buffer pool, max 64.

**Requires restart**.

### innodb_log_file_size (Redo Log)
Controls the size of redo log files. Larger = better write performance but longer crash recovery.

| Workload | Recommended |
|----------|------------|
| OLTP (high writes) | 1-2GB |
| OLAP (read-heavy) | 256MB-512MB |
| Mixed | 512MB-1GB |

**Check with**: `get_innodb_status` → if "Log sequence number" minus "Last checkpoint at" approaches total redo log size, increase this value.

**Requires restart** (MySQL 5.7). In MySQL 8.0.30+, use `ALTER INSTANCE DISABLE INNODB REDO_LOG` workflow.

### innodb_flush_log_at_trx_commit

| Value | Durability | Performance | Use Case |
|-------|-----------|-------------|----------|
| 1 | Full ACID (no data loss on crash) | Slowest | Production / financial data |
| 2 | Lose up to 1 second on OS crash | Faster | Most production workloads |
| 0 | Lose up to 1 second on MySQL crash | Fastest | Non-critical data / batch imports |

**Dynamic**: `SET GLOBAL innodb_flush_log_at_trx_commit = 1;`

### innodb_io_capacity / innodb_io_capacity_max
Tell InnoDB how much I/O bandwidth is available.

| Storage Type | io_capacity | io_capacity_max |
|-------------|-------------|-----------------|
| HDD (7200 RPM) | 100-200 | 400 |
| SAS (15K RPM) | 400-800 | 1600 |
| SSD (SATA) | 2000-4000 | 8000 |
| NVMe SSD | 4000-10000 | 20000 |
| Cloud (GP3/io2) | 3000-16000 | 32000 |

**Dynamic**: `SET GLOBAL innodb_io_capacity = 2000;`

### innodb_flush_method

| OS | Recommended | Why |
|----|------------|-----|
| Linux | `O_DIRECT` | Avoids double-buffering with OS cache |
| Windows | (default) | `O_DIRECT` not available |

**Requires restart**.

---

## Memory Settings

### Per-Thread Buffers
These are allocated per connection, so total memory = value * max_connections.

| Setting | Conservative | Moderate | Aggressive | Notes |
|---------|-------------|----------|------------|-------|
| `sort_buffer_size` | 256K | 512K | 2M | Only increase if Sort_merge_passes is high |
| `join_buffer_size` | 256K | 512K | 1M | Only helps joins without indexes |
| `read_buffer_size` | 128K | 256K | 512K | Sequential scan buffer |
| `read_rnd_buffer_size` | 256K | 512K | 1M | Random read buffer after sort |
| `thread_stack` | 256K | 256K | 512K | Rarely needs changing |

**Important**: Do NOT set these to large values (>4MB). They are allocated per-connection and large values waste memory.

**Check with**: `calculate_memory_usage` → shows total memory impact.

### tmp_table_size / max_heap_table_size
Must be set to the same value. The effective limit is the smaller of the two.

| Workload | Recommended |
|----------|------------|
| OLTP (simple queries) | 64M |
| OLAP (complex GROUP BY) | 128-256M |
| Mixed | 64-128M |

**Check with**: `analyze_temp_tables` → if disk_tmp_pct > 25%, consider increasing (but also optimize the queries).

**Dynamic**: `SET GLOBAL tmp_table_size = 67108864; SET GLOBAL max_heap_table_size = 67108864;`

---

## Connection Settings

### max_connections

| Server Size | Recommended | Notes |
|------------|------------|-------|
| Small (dev) | 50-100 | |
| Medium (production) | 150-300 | |
| Large (production) | 300-500 | |
| Very large | 500-1000 | Use connection pooling middleware |

**Formula**: Actual peak concurrent connections + 20% headroom. Never set to 10,000 "just in case" - each connection reserves memory.

**Check with**: `analyze_connections` → `max_used_connections` shows the actual historical peak.

**Dynamic**: `SET GLOBAL max_connections = 300;`

### wait_timeout / interactive_timeout
How long idle connections stay open before MySQL closes them.

| Scenario | Recommended |
|----------|------------|
| With connection pooling | 300 (5 minutes) |
| Without connection pooling | 600-1800 |
| Default (often too high) | 28800 (8 hours) |

**Check with**: `analyze_connections` → if most connections are sleeping, reduce these values.

**Dynamic**: `SET GLOBAL wait_timeout = 300;`

### thread_cache_size
Caches threads for reuse instead of creating new ones per connection.

| Connections/sec | Recommended |
|----------------|------------|
| < 10 | 8 |
| 10-50 | 16 |
| > 50 | 32-64 |

**Check**: If `Threads_created / Connections > 0.01`, increase `thread_cache_size`.

**Dynamic**: `SET GLOBAL thread_cache_size = 16;`

---

## Query and Optimizer Settings

### long_query_time
Threshold in seconds for the slow query log.

| Environment | Recommended |
|------------|------------|
| Development | 0.5 (500ms) |
| Production (initial tuning) | 1.0 |
| Production (already tuned) | 0.1-0.5 |

**Dynamic**: `SET GLOBAL long_query_time = 1;`

### eq_range_index_dive_limit
Controls when the optimizer switches from index dives to index statistics for IN() lists.

| Scenario | Recommended |
|----------|------------|
| Default | 200 |
| Large IN() lists common | 0 (always use statistics) |

Setting to 0 avoids slow planning for queries like `WHERE id IN (1, 2, ..., 10000)`.

### optimizer_switch flags
Common adjustments:

| Flag | Default | When to Change |
|------|---------|---------------|
| `mrr=on` | off | Enable for range-heavy workloads |
| `batched_key_access=on` | off | Enable for join-heavy workloads (requires mrr=on) |
| `derived_merge=on` | on | Leave on unless causing issues |
| `index_merge=on` | on | Leave on for complex WHERE clauses |

**Check with**: `review_optimizer_config` → lists all switches and recommendations.

---

## Logging Settings

### Slow Query Log

```ini
slow_query_log = ON
long_query_time = 1
log_queries_not_using_indexes = ON
log_slow_admin_statements = ON
slow_query_log_file = /var/log/mysql/slow.log
```

### General Query Log (Use Sparingly)

```ini
general_log = OFF          # Only enable temporarily for debugging
general_log_file = /var/log/mysql/general.log
```

**Warning**: The general query log captures ALL queries and causes significant I/O overhead. Never leave it on in production.

### Performance Schema

```ini
performance_schema = ON
performance_schema_max_digest_length = 4096
performance_schema_max_sql_text_length = 4096
```

Performance Schema is required for most diagnostic tools. The overhead is typically 5-10% of total server resources.

---

## Replication Settings

### Parallel Replication (MySQL 5.7+)

```ini
replica_parallel_workers = 4       # Start with 4, increase to 8-16 if lag persists
replica_parallel_type = LOGICAL_CLOCK
replica_preserve_commit_order = ON  # Maintains consistency
```

### Binary Log

```ini
binlog_format = ROW               # Most reliable for replication
binlog_row_image = FULL           # MINIMAL saves space but limits point-in-time recovery
sync_binlog = 1                   # Full durability
binlog_expire_logs_seconds = 604800  # 7 days retention
```

### GTID (Recommended for Modern Setups)

```ini
gtid_mode = ON
enforce_gtid_consistency = ON
```

---

## Security Settings

```ini
# Password validation
validate_password.policy = MEDIUM
validate_password.length = 8

# SSL/TLS
require_secure_transport = ON

# Disable local file loading
local_infile = OFF

# Bind to specific interface
bind_address = 127.0.0.1          # Or specific IP
```

---

## Workload-Specific Profiles

### OLTP (Online Transaction Processing)
High-concurrency, short queries, many reads and writes.

```ini
innodb_buffer_pool_size = 70% of RAM
innodb_buffer_pool_instances = 8-16
innodb_log_file_size = 1G
innodb_flush_log_at_trx_commit = 1
innodb_io_capacity = 2000
innodb_io_capacity_max = 4000
max_connections = 300
sort_buffer_size = 256K
join_buffer_size = 256K
tmp_table_size = 64M
max_heap_table_size = 64M
```

### OLAP (Analytics / Reporting)
Few connections, complex queries, large result sets.

```ini
innodb_buffer_pool_size = 80% of RAM
innodb_buffer_pool_instances = 8
innodb_log_file_size = 512M
innodb_flush_log_at_trx_commit = 2
max_connections = 50
sort_buffer_size = 2M
join_buffer_size = 1M
read_rnd_buffer_size = 1M
tmp_table_size = 256M
max_heap_table_size = 256M
```

### Mixed Workload
Balance between OLTP and OLAP.

```ini
innodb_buffer_pool_size = 70% of RAM
innodb_buffer_pool_instances = 8
innodb_log_file_size = 1G
innodb_flush_log_at_trx_commit = 1
max_connections = 200
sort_buffer_size = 512K
join_buffer_size = 512K
tmp_table_size = 128M
max_heap_table_size = 128M
```

---

## Advanced Configuration

### Optimizer Trace Setup

Enable optimizer trace per-session to diagnose plan selection. Never enable globally in production.

```ini
# Per-session only:
SET optimizer_trace = "enabled=on";
SET optimizer_trace_max_mem_size = 1048576;  -- 1MB for complex queries
# Run your query, then:
SELECT * FROM information_schema.OPTIMIZER_TRACE\G
SET optimizer_trace = "enabled=off";
```

See [advanced-diagnostics.md](./advanced-diagnostics.md) for detailed optimizer trace analysis.

### Performance Schema Memory Instruments

Enable memory instrumentation when diagnosing memory consumers:

```ini
# Enable memory instruments (dynamic, takes effect for new allocations only)
UPDATE performance_schema.setup_instruments
SET ENABLED = 'YES', TIMED = 'YES'
WHERE NAME LIKE 'memory/%';

# Key memory summary views:
# - memory_summary_global_by_event_name (top memory consumers)
# - memory_summary_by_thread_by_event_name (per-thread)
# - memory_summary_by_account_by_event_name (per-user)
```

> **Impact**: Memory instrumentation adds ~5% overhead. Enable selectively (e.g., `memory/innodb/%` only) for long-term monitoring.

### Cost Model Tuning

MySQL 8.0 cost model can be adjusted for SSD vs HDD and buffer pool hit rate:

```ini
# Check current cost model:
SELECT * FROM mysql.server_cost;
SELECT * FROM mysql.engine_cost;

# For SSD storage (lower random I/O cost):
UPDATE mysql.engine_cost
SET cost_value = 1.0
WHERE cost_name = 'io_block_read_cost';

# For large buffer pool with high hit rate:
UPDATE mysql.engine_cost
SET cost_value = 0.25
WHERE cost_name = 'memory_block_read_cost';

FLUSH OPTIMIZER_COSTS;
```

| Scenario | io_block_read_cost | memory_block_read_cost |
|----------|-------------------|----------------------|
| HDD storage | 1.0 (default) | 0.25 (default) |
| SSD storage | 0.5 - 1.0 | 0.25 |
| Large buffer pool (>80% hit) | 1.0 | 0.1 |
| Tiny buffer pool (<60% hit) | 1.0 | 0.5 |

### InnoDB Purge Configuration

Control how aggressively InnoDB purges obsolete MVCC versions:

```ini
# Number of purge threads (MySQL 8.0 default: 4)
innodb_purge_threads = 4

# Maximum undo log history length before throttling DML
innodb_max_purge_lag = 0            # Default: 0 (no throttle)
# Recommendation for high-write workloads:
innodb_max_purge_lag = 1000000      # Throttle DML when history > 1M

# Delay per transaction in microseconds when purge lag exceeds threshold
innodb_max_purge_lag_delay = 0      # Default: 0
# Recommendation:
innodb_max_purge_lag_delay = 300000 # Max 300ms delay per transaction
```

**When to tune purge**: Check `SHOW ENGINE INNODB STATUS` → History list length. If consistently > 10000, increase purge threads or set max_purge_lag to prevent unbounded growth.

### Adaptive Hash Index (AHI) Guidance

AHI is an in-memory structure that can accelerate point lookups but may cause contention:

```ini
# Check AHI status:
SHOW GLOBAL STATUS LIKE 'Innodb_adaptive_hash%';

# AHI hit rate:
# hit_rate = searches / (searches + non_searches)
# If hit rate < 50%, AHI is consuming memory without benefit

# Disable AHI (dynamic, takes effect immediately):
SET GLOBAL innodb_adaptive_hash_index = OFF;

# Partitions (reduces contention on multi-core, default: 8):
# innodb_adaptive_hash_index_parts = 8  (static, requires restart)
```

| Condition | Recommendation |
|-----------|---------------|
| AHI hit rate > 80%, low contention | Keep enabled (default) |
| AHI hit rate < 50% | Disable — saves memory, reduces mutex waits |
| Many concurrent point lookups | Increase innodb_adaptive_hash_index_parts to 16 |
| Range scans dominate workload | Disable — AHI doesn't help range scans |

---

## Reporting Format

When recommending configuration changes, use this format for clarity:

```
Setting: innodb_buffer_pool_size
Current: 128M
Recommended: 22G (70% of 32GB RAM)
Impact: High - buffer pool hit ratio will improve from 94% to ~99%+
Dynamic: Yes (SET GLOBAL innodb_buffer_pool_size = 23622320128;)
Restart Required: No (MySQL 5.7+)
```

Always distinguish between:
- **Dynamic settings** (can be changed with SET GLOBAL, takes effect immediately or gradually)
- **Static settings** (require a MySQL restart, must be changed in my.cnf/my.ini)
