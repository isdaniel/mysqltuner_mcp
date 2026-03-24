# MCP Tool Quick Reference

All 39 tools provided by the mysqltuner_mcp MCP server, organized by category.

---

## Performance Analysis (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `get_slow_queries` | Top slow queries from performance_schema | `order_by` (total_time/avg_time/calls), `schema`, `limit` |
| `analyze_query` | Run EXPLAIN on a query (JSON/tree/traditional) | `query` (required), `format`, `schema` |
| `get_table_stats` | Table and index metadata from information_schema | `schema`, `table_name` |

**When to use**: Start here when investigating query performance. `get_slow_queries` identifies the worst offenders, `analyze_query` deep-dives into one query's execution plan.

---

## Index Tools (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `get_index_recommendations` | Suggest missing indexes from query patterns | `schema`, `limit` |
| `find_unused_indexes` | Find zero-read, duplicate, redundant indexes | `schema` |
| `get_index_stats` | Index cardinality, selectivity, I/O stats | `schema`, `table_name` |

**When to use**: After finding slow queries, use these to fix the root cause. Always run `find_unused_indexes` before `get_index_recommendations` - remove waste first, then add new indexes.

---

## Health Monitoring (4 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `check_database_health` | Health score (0-100) with breakdown | (none) |
| `get_active_queries` | Currently running queries from PROCESSLIST | `min_duration`, `include_sleeping` |
| `review_settings` | Configuration review against best practices | `category` (memory/innodb/connections/logging/replication) |
| `analyze_wait_events` | Wait event analysis from performance_schema | `category` (io/lock/buffer/log) |

**When to use**: `check_database_health` is always the first tool to call for any tuning session. `analyze_wait_events` reveals the true bottleneck type.

---

## InnoDB Analysis (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `get_innodb_status` | Parse SHOW ENGINE INNODB STATUS | (none) |
| `analyze_buffer_pool` | Buffer pool pages, hit ratio, per-table breakdown | (none) |
| `analyze_innodb_transactions` | Active transactions, lock waits | (none) |

**When to use**: For InnoDB-specific tuning. Buffer pool hit ratio is the single most important InnoDB metric. Transactions tool helps find long-running transactions holding locks.

---

## Statement Analysis (6 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `analyze_statements` | Top statements from sys.statement_analysis | `schema`, `limit` |
| `get_statements_with_temp_tables` | Queries creating disk temp tables | `schema`, `limit` |
| `get_statements_with_sorting` | Queries with expensive sorts | `schema`, `limit` |
| `get_statements_with_full_scans` | Queries doing full table scans | `schema`, `limit` |
| `get_statements_with_errors` | Queries generating errors/warnings | `schema`, `limit` |
| `analyze_long_queries_for_type_collation_issues` | Implicit type/collation mismatches | `schema`, `limit` |

**When to use**: These tools find specific anti-patterns in queries. Run them all during a comprehensive audit. The type/collation tool catches hidden performance killers that EXPLAIN alone won't reveal.

---

## Memory Analysis (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `calculate_memory_usage` | Global + per-thread memory calculation | (none) |
| `get_memory_by_host` | Memory usage by client host | (none) |
| `get_table_memory_usage` | Table cache and buffer pool per-table | (none) |

**When to use**: Use `calculate_memory_usage` early in any audit to verify MySQL isn't over-allocated relative to physical RAM. Per-host analysis helps identify applications consuming disproportionate resources.

---

## Storage Engine (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `analyze_storage_engines` | Engine distribution, non-InnoDB detection | (none) |
| `get_fragmented_tables` | Tables with high DATA_FREE ratio | `schema`, `min_fragmentation` |
| `analyze_auto_increment` | Auto-increment overflow risk detection | `schema`, `threshold` (default: 75%) |

**When to use**: `get_fragmented_tables` during storage audits. `analyze_auto_increment` is critical for high-insert-rate tables - INT columns max out at ~2.1 billion rows.

---

## Replication (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `get_replication_status` | Master/replica status and lag | (none) |
| `get_galera_status` | Galera cluster (wsrep) status | (none) |
| `get_group_replication_status` | MySQL Group Replication | (none) |

**When to use**: Only when the server is part of a replication topology. `get_replication_status` covers standard async/semi-sync replication. Use the Galera/GR tools for their respective cluster types.

---

## Security (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `analyze_security` | Security score with 8 checks | (none) |
| `analyze_user_privileges` | Per-user privilege breakdown | `user`, `host` |
| `check_audit_log` | Audit plugin enablement check | (none) |

**When to use**: Include in every comprehensive audit. Security and performance are related - overprivileged users and no audit logging are risks.

---

## Diagnostic (5 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `analyze_connections` | Connection states, per-user/host breakdown | `group_by` (user/host/state/database), `include_sleeping` |
| `analyze_table_locks` | Table lock contention, metadata locks | `schema` |
| `analyze_temp_tables` | Temp table disk spill analysis | `top_n` |
| `check_perf_schema_config` | Performance Schema enablement check | `verbose` |
| `review_optimizer_config` | Optimizer switches and cost model | `include_cost_model` |

**When to use**: `check_perf_schema_config` should be the very first tool called. `analyze_connections` when diagnosing connection issues. `review_optimizer_config` when queries have suboptimal plans despite good indexes.

---

## Schema & Binlog (3 tools)

| Tool | Description | Key Arguments |
|------|-------------|---------------|
| `profile_schema_sizes` | Database/table size profiling | `schema`, `top_n` |
| `analyze_binlog` | Binary log config and throughput | (none) |
| `get_global_status_snapshot` | Curated global status counters | `category` (all/throughput/innodb/connections/query_quality/handlers) |

**When to use**: `profile_schema_sizes` for capacity planning. `get_global_status_snapshot` for throughput baseline - call it twice with a delay to compute delta rates.

---

## Tips for Effective Tool Usage

1. **Always start with `check_perf_schema_config`** before any diagnostic session
2. **Use `check_database_health` for quick triage** - the score guides urgency
3. **Call `get_global_status_snapshot` twice** with a short delay to compute per-second delta rates
4. **Run `find_unused_indexes` before `get_index_recommendations`** - remove waste first
5. **Use `analyze_query` with format='json'** for the most detailed EXPLAIN output
6. **Check `analyze_connections` when seeing connection errors** - state breakdown reveals idle bloat
7. **Use `review_optimizer_config`** when plans are suboptimal despite proper indexes
