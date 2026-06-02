"""
MySQL tuning tools package.

Contains tool handlers for:
- Performance analysis
- Index tuning
- Health monitoring
- InnoDB analysis
- Statement analysis
- Memory calculations
- Storage engine analysis
- Replication status
- Security analysis
- Diagnostic analysis
- Schema profiling
"""

from .toolhandler import ToolHandler
from .tools_health import (
    ActiveQueriesToolHandler,
    DatabaseHealthToolHandler,
    SettingsReviewToolHandler,
    WaitEventsToolHandler,
)
from .tools_index import (
    IndexRecommendationsToolHandler,
    IndexStatsToolHandler,
    UnusedIndexesToolHandler,
)
from .tools_performance import (
    AnalyzeQueryToolHandler,
    CompareExplainPlansToolHandler,
    GetSlowQueriesToolHandler,
    TableIoHotspotsToolHandler,
    TableStatsToolHandler,
)
from .tools_innodb import (
    InnoDBStatusToolHandler,
    InnoDBBufferPoolToolHandler,
    InnoDBRedoLogPressureToolHandler,
    InnoDBTransactionsToolHandler,
)
from .tools_statements import (
    StatementAnalysisToolHandler,
    StatementsTempTablesToolHandler,
    StatementsSortingToolHandler,
    StatementsFullScansToolHandler,
    StatementErrorsToolHandler,
    LongQueryTypeCollationIssuesToolHandler,
    TempTableSpillsInProgressToolHandler,
)
from .tools_memory import (
    MemoryCalculationsToolHandler,
    MemoryByHostToolHandler,
    TableMemoryUsageToolHandler,
)
from .tools_engines import (
    StorageEngineAnalysisToolHandler,
    FragmentedTablesToolHandler,
    AutoIncrementAnalysisToolHandler,
)
from .tools_replication import (
    ReplicationStatusToolHandler,
    GaleraClusterToolHandler,
    GroupReplicationToolHandler,
)
from .tools_security import (
    SecurityAnalysisToolHandler,
    UserPrivilegesToolHandler,
    AuditLogToolHandler,
)
from .tools_diagnostic import (
    ConnectionAnalysisToolHandler,
    LockWaitGraphToolHandler,
    TableLockAnalysisToolHandler,
    TempTableAnalysisToolHandler,
    PerfSchemaConfigToolHandler,
    OptimizerConfigToolHandler,
)
from .tools_schema import (
    SchemaProfilingToolHandler,
    BinlogAnalysisToolHandler,
    GlobalStatusSnapshotToolHandler,
)

__all__ = [
    # Base
    "ToolHandler",
    # Performance
    "GetSlowQueriesToolHandler",
    "AnalyzeQueryToolHandler",
    "TableStatsToolHandler",
    "CompareExplainPlansToolHandler",
    "TableIoHotspotsToolHandler",
    # Index
    "IndexRecommendationsToolHandler",
    "UnusedIndexesToolHandler",
    "IndexStatsToolHandler",
    # Health
    "DatabaseHealthToolHandler",
    "ActiveQueriesToolHandler",
    "SettingsReviewToolHandler",
    "WaitEventsToolHandler",
    # InnoDB
    "InnoDBStatusToolHandler",
    "InnoDBBufferPoolToolHandler",
    "InnoDBTransactionsToolHandler",
    "InnoDBRedoLogPressureToolHandler",
    # Statement Analysis
    "StatementAnalysisToolHandler",
    "StatementsTempTablesToolHandler",
    "StatementsSortingToolHandler",
    "StatementsFullScansToolHandler",
    "StatementErrorsToolHandler",
    "LongQueryTypeCollationIssuesToolHandler",
    "TempTableSpillsInProgressToolHandler",
    # Memory
    "MemoryCalculationsToolHandler",
    "MemoryByHostToolHandler",
    "TableMemoryUsageToolHandler",
    # Storage Engines
    "StorageEngineAnalysisToolHandler",
    "FragmentedTablesToolHandler",
    "AutoIncrementAnalysisToolHandler",
    # Replication
    "ReplicationStatusToolHandler",
    "GaleraClusterToolHandler",
    "GroupReplicationToolHandler",
    # Security
    "SecurityAnalysisToolHandler",
    "UserPrivilegesToolHandler",
    "AuditLogToolHandler",
    # Diagnostic
    "ConnectionAnalysisToolHandler",
    "TableLockAnalysisToolHandler",
    "TempTableAnalysisToolHandler",
    "PerfSchemaConfigToolHandler",
    "OptimizerConfigToolHandler",
    "LockWaitGraphToolHandler",
    # Schema & Binlog
    "SchemaProfilingToolHandler",
    "BinlogAnalysisToolHandler",
    "GlobalStatusSnapshotToolHandler",
]
