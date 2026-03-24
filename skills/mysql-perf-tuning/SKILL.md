---
name: mysql-perf-tuning
description: MySQL performance tuning skill using the mysqltuner_mcp MCP server. Use this skill whenever the user asks about MySQL performance, slow queries, database optimization, MySQL configuration tuning, index optimization, InnoDB tuning, connection issues, lock contention, replication lag, memory sizing, or any MySQL health/diagnostic task. Also use when the user mentions mysqltuner, mysqltunermcp, database is slow, query optimization, EXPLAIN analysis, buffer pool, or needs help understanding MySQL metrics. This skill teaches the systematic methodology for diagnosing and resolving MySQL performance issues.
---

# MySQL Performance Tuning with mysqltuner_mcp

A systematic approach to MySQL performance tuning using the `mysqltuner_mcp` MCP server, which provides 39 diagnostic tools for analyzing and optimizing MySQL databases.

## Prerequisites

This skill requires the **mysqltuner_mcp** MCP server to be connected. The server provides tools that query MySQL's `performance_schema`, `information_schema`, `sys` schema, and server status variables.

Before starting any tuning work, verify the MCP connection is active by calling one of the tools (e.g., `check_database_health`).

---

## Decision Tree: Routing to the Right Analysis

```
User's problem → What type of issue?
    │
    ├─ "Database is slow" (general) ──────────→ Full Performance Audit
    │   Start: check_database_health → get_slow_queries → analyze_wait_events
    │   Reference: [tuning-workflow.md] Phase 2-6
    │
    ├─ Specific slow query ────────────────────→ Query Optimization
    │   Start: analyze_query (EXPLAIN) → get_index_recommendations
    │   Reference: [tuning-workflow.md] "Query Optimization Workflow"
    │
    ├─ "Too many connections" / timeouts ──────→ Connection Analysis
    │   Start: analyze_connections → calculate_memory_usage
    │   Reference: [common-problems.md] "Too Many Connections"
    │
    ├─ Lock waits / deadlocks ─────────────────→ Lock Diagnosis
    │   Start: analyze_table_locks → analyze_innodb_transactions
    │   Reference: [common-problems.md] "Lock Contention"
    │
    ├─ Replication lag ────────────────────────→ Replication Analysis
    │   Start: get_replication_status → get_slow_queries
    │   Reference: [common-problems.md] "Replication Lag"
    │
    ├─ Memory / OOM issues ────────────────────→ Memory Analysis
    │   Start: calculate_memory_usage → analyze_buffer_pool
    │   Reference: [configuration-recommendations.md] "Memory Settings"
    │
    ├─ Disk space issues ──────────────────────→ Storage Analysis
    │   Start: profile_schema_sizes → get_fragmented_tables → analyze_binlog
    │   Reference: [common-problems.md] "Disk Space"
    │
    ├─ Configuration review ───────────────────→ Settings Audit
    │   Start: review_settings → review_optimizer_config
    │   Reference: [configuration-recommendations.md]
    │
    ├─ Index optimization ─────────────────────→ Index Review
    │   Start: find_unused_indexes → get_index_recommendations
    │   Reference: [tuning-workflow.md] "Index Optimization"
    │
    ├─ Security audit ─────────────────────────→ Security Analysis
    │   Start: analyze_security → analyze_user_privileges
    │
    ├─ Health check / monitoring ──────────────→ Quick Health
    │   Start: check_database_health → get_global_status_snapshot
    │
    ├─ Table maintenance / fragmentation ─────→ Maintenance
    │   Start: get_fragmented_tables → profile_schema_sizes → analyze_auto_increment
    │   Reference: [common-problems.md] "Table Fragmentation", [advanced-diagnostics.md] "Table Maintenance"
    │
    ├─ DDL stuck / metadata lock ─────────────→ Metadata Lock Diagnosis
    │   Start: analyze_table_locks → get_active_queries → analyze_innodb_transactions
    │   Reference: [common-problems.md] "Metadata Lock Blocking DDL"
    │
    └─ Advanced / beyond MCP tools ───────────→ Raw SQL Diagnosis
        Reference: [tsg-diagnostic-queries.md], [advanced-diagnostics.md]
        Covers: optimizer trace, errant GTIDs, DEFINER audits, delta comparison
```

---

## The First Thing to Do: Always Start Here

**Step 1: Check prerequisites**

```
Tool: check_perf_schema_config
```

Many tools depend on `performance_schema`. If it is OFF, warn the user that diagnostic depth will be limited and recommend enabling it.

**Step 2: Get the health score**

```
Tool: check_database_health
```

This returns a score from 0-100:
- **90-100**: Healthy server. Focus on fine-tuning and proactive optimization.
- **70-89**: Warning zone. Specific issues need attention but server is functional.
- **50-69**: Degraded. Multiple issues are actively impacting performance.
- **0-49**: Critical. Immediate action required to prevent outages.

The health score tells you how urgently to act and helps prioritize the analysis.

---

## Core Tuning Methodology

The tuning process follows a priority order based on impact-to-effort ratio. Always work top-down unless the user has a specific problem to solve.

### Priority 1: Query Optimization (Highest Impact)

Bad queries are the #1 cause of MySQL performance problems. A single unindexed query can bring down an entire server.

```
Tools (in order):
1. get_slow_queries              → Find the costliest queries
2. get_statements_with_full_scans → Find queries missing indexes
3. analyze_query (per query)     → Get EXPLAIN plan details
4. get_index_recommendations     → Generate CREATE INDEX statements
```

For detailed methodology, read [reference/tuning-workflow.md](./reference/tuning-workflow.md) → "Query Optimization Workflow".

### Priority 2: Index Optimization (High Impact, Low Effort)

Remove waste before adding new indexes. Every unnecessary index slows down writes.

```
Tools:
1. find_unused_indexes           → Find zero-read indexes to DROP
2. get_index_recommendations     → Suggest missing indexes to CREATE
3. get_index_stats               → Verify index selectivity
```

### Priority 3: Buffer Pool and Memory (High Impact, Low Effort)

```
Tools:
1. analyze_buffer_pool           → Check hit ratio (target: >99% for OLTP)
2. calculate_memory_usage        → Verify memory allocation vs physical RAM
3. review_settings (category: memory/innodb) → Configuration review
```

### Priority 4: Connections and Locks (Medium Impact)

```
Tools:
1. analyze_connections           → Connection state breakdown
2. analyze_table_locks           → Lock contention patterns
3. analyze_temp_tables           → Temp table disk spill analysis
```

### Priority 5: Storage and Capacity (Lower Impact)

```
Tools:
1. profile_schema_sizes          → Data distribution
2. get_fragmented_tables         → Space reclamation opportunities
3. analyze_auto_increment        → Overflow risk detection
4. analyze_binlog                → Binlog disk usage
```

---

## How to Present Results

When reporting findings to the user, follow this structure:

1. **Health Score and Summary** - Start with the overall score and 1-2 sentence summary
2. **Critical Issues** - Anything requiring immediate action (blocking, data-loss risk)
3. **Top Recommendations** - Ranked by impact, with specific values and SQL statements
4. **Configuration Changes** - Show exact `SET GLOBAL` or `my.cnf` values
5. **Index Changes** - Provide complete `CREATE INDEX` and `DROP INDEX` DDL
6. **Monitoring Notes** - What to watch going forward

For configuration recommendations with specific values, read [reference/configuration-recommendations.md](./reference/configuration-recommendations.md).

---

## Available MCP Prompts

The MCP server provides pre-built prompts for common scenarios. Suggest these to the user when appropriate:

| Prompt | When to Use |
|--------|------------|
| `performance_audit` | Comprehensive full-server audit |
| `optimize_slow_query` | Analyzing a specific slow query |
| `health_check` | Quick overall health assessment |
| `index_review` | Schema-wide index optimization |
| `connection_tuning` | Connection pool and timeout issues |
| `innodb_deep_dive` | Buffer pool, redo log, transaction tuning |
| `lock_contention_diagnosis` | Deadlocks, lock waits, metadata locks |
| `capacity_planning` | Growth projections and resource sizing |

---

## Reference Files

Load these as needed for deeper guidance:

- **[reference/tuning-workflow.md](./reference/tuning-workflow.md)** - Detailed step-by-step methodology for each tuning phase, including the query optimization workflow and InnoDB deep-dive process
- **[reference/tool-reference.md](./reference/tool-reference.md)** - Quick reference card for all 39 MCP tools organized by category, with descriptions and key use cases
- **[reference/common-problems.md](./reference/common-problems.md)** - Symptom-to-solution mapping for the most common MySQL performance problems, with exact tool sequences for each
- **[reference/configuration-recommendations.md](./reference/configuration-recommendations.md)** - Specific MySQL configuration values and formulas for different workload types (OLTP, OLAP, mixed), organized by category
- **[reference/tsg-diagnostic-queries.md](./reference/tsg-diagnostic-queries.md)** - Diagnostic SQL queries from TSGs mapped to MCP tools, parameter tuning matrix, and raw SQL for scenarios beyond tool coverage
- **[reference/advanced-diagnostics.md](./reference/advanced-diagnostics.md)** - Advanced techniques: optimizer trace, table maintenance, schema change impact, error log patterns, historical delta comparison, security deep dive, replication advanced troubleshooting

---

## Guidelines

- Always verify `performance_schema` is enabled before relying on diagnostic tools
- Present numbers in context (e.g., "Buffer pool hit ratio is 94% - below the 99% target for OLTP workloads")
- Provide actionable SQL statements, not just advice (e.g., give the actual `CREATE INDEX` DDL)
- When recommending configuration changes, show both the current value and the recommended value
- Warn about changes that require a MySQL restart vs those that can be set dynamically
- For production servers, recommend testing changes on staging first
- If the health score is critical (<50), focus on the top 1-2 issues rather than doing a comprehensive audit

---

## When to Go Beyond MCP Tools

The 39 MCP tools cover live performance diagnostics comprehensively. For these scenarios, use raw SQL from the reference files:

| Scenario | Reference File | Key Technique |
|----------|---------------|---------------|
| EXPLAIN doesn't explain bad plan choice | [advanced-diagnostics.md](./reference/advanced-diagnostics.md) | Optimizer trace |
| Need to compare metrics over time | [tsg-diagnostic-queries.md](./reference/tsg-diagnostic-queries.md) | Global status delta comparison |
| Table maintenance after bulk operations | [advanced-diagnostics.md](./reference/advanced-diagnostics.md) | ANALYZE / OPTIMIZE / CHECK TABLE |
| DDL stuck on metadata lock | [common-problems.md](./reference/common-problems.md) | Metadata lock diagnosis |
| Security audit of DEFINERs / routines | [advanced-diagnostics.md](./reference/advanced-diagnostics.md) | DEFINER / SQL SECURITY audit |
| Replication errant GTIDs | [advanced-diagnostics.md](./reference/advanced-diagnostics.md) | GTID_SUBTRACT comparison |
| Parallel replication not parallelizing | [tsg-diagnostic-queries.md](./reference/tsg-diagnostic-queries.md) | Worker utilization + missing PK check |
| InnoDB purge lag / undo growth | [common-problems.md](./reference/common-problems.md) | Purge lag diagnosis |
| InnoDB mutex contention | [advanced-diagnostics.md](./reference/advanced-diagnostics.md) | SHOW ENGINE INNODB MUTEX |
| Histogram statistics for better plans | [advanced-diagnostics.md](./reference/advanced-diagnostics.md) | ANALYZE TABLE ... UPDATE HISTOGRAM |
