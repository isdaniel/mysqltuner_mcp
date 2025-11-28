"""
Replication status tool handlers for MySQL.

Includes tools for analyzing replication:
- Master/Source status
- Slave/Replica status
- Replication lag analysis
- Galera cluster status (MariaDB)
- Group replication status (MySQL)

Based on MySQLTuner's replication analysis patterns.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class ReplicationStatusToolHandler(ToolHandler):
    """Tool handler for replication status analysis."""

    name = "get_replication_status"
    title = "Replication Status"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get MySQL replication status and health.

Analyzes:
- Master/Source status and binary log position
- Slave/Replica status and lag
- Replication errors and warnings
- Binary log configuration
- Semi-sync replication status

Works with both MySQL and MariaDB terminology."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "check_all_channels": {
                        "type": "boolean",
                        "description": "Check all replication channels (multi-source)",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            check_all = arguments.get("check_all_channels", True)

            output = {
                "is_master": False,
                "is_slave": False,
                "master_status": {},
                "slave_status": [],
                "binary_logs": {},
                "replication_variables": {},
                "issues": [],
                "recommendations": []
            }

            variables = await self.sql_driver.get_server_variables()
            status = await self.sql_driver.get_server_status()

            # Check if binary logging is enabled (potential master)
            log_bin = variables.get("log_bin", "OFF")
            output["binary_logs"]["log_bin"] = log_bin

            if log_bin.upper() == "ON":
                await self._get_master_status(output)

            # Check slave status
            await self._get_slave_status(output, check_all)

            # Get replication-related variables
            await self._get_replication_variables(variables, output)

            # Check for semi-sync replication
            await self._check_semisync(variables, status, output)

            # Generate recommendations
            self._generate_recommendations(output)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _get_master_status(self, output: dict) -> None:
        """Get master/source status."""
        try:
            # Try MySQL 8.0+ syntax first
            try:
                result = await self.sql_driver.execute_query(
                    "SHOW BINARY LOG STATUS"
                )
            except Exception:
                # Fall back to older syntax
                result = await self.sql_driver.execute_query("SHOW MASTER STATUS")

            if result:
                row = result[0]
                output["is_master"] = True
                output["master_status"] = {
                    "file": row.get("File"),
                    "position": row.get("Position"),
                    "binlog_do_db": row.get("Binlog_Do_DB"),
                    "binlog_ignore_db": row.get("Binlog_Ignore_DB"),
                    "executed_gtid_set": row.get("Executed_Gtid_Set")
                }

                # Get binary log list
                try:
                    logs = await self.sql_driver.execute_query("SHOW BINARY LOGS")
                    total_size = sum(log.get("File_size", 0) for log in logs)
                    output["binary_logs"]["files_count"] = len(logs)
                    output["binary_logs"]["total_size_mb"] = round(
                        total_size / 1024 / 1024, 2
                    )
                    output["binary_logs"]["latest_files"] = [
                        {
                            "name": log.get("Log_name"),
                            "size_mb": round(
                                (log.get("File_size") or 0) / 1024 / 1024, 2
                            )
                        }
                        for log in logs[-5:]  # Last 5 files
                    ]
                except Exception:
                    pass

                # Get connected slaves
                try:
                    # Try MySQL 8.0.22+ syntax
                    try:
                        slaves = await self.sql_driver.execute_query(
                            "SHOW REPLICAS"
                        )
                    except Exception:
                        slaves = await self.sql_driver.execute_query(
                            "SHOW SLAVE HOSTS"
                        )

                    output["master_status"]["connected_replicas"] = len(slaves)
                    output["master_status"]["replicas"] = [
                        {
                            "server_id": s.get("Server_id"),
                            "host": s.get("Host"),
                            "port": s.get("Port"),
                            "uuid": s.get("Replica_UUID") or s.get("Slave_UUID")
                        }
                        for s in slaves
                    ]
                except Exception:
                    pass

        except Exception:
            # Not a master or no privileges
            pass

    async def _get_slave_status(self, output: dict, check_all: bool) -> None:
        """Get slave/replica status."""
        try:
            # Try MySQL 8.0.22+ syntax first
            try:
                if check_all:
                    results = await self.sql_driver.execute_query(
                        "SHOW REPLICA STATUS"
                    )
                else:
                    results = await self.sql_driver.execute_query(
                        "SHOW REPLICA STATUS FOR CHANNEL ''"
                    )
            except Exception:
                # Fall back to older syntax
                results = await self.sql_driver.execute_query("SHOW SLAVE STATUS")

            if results:
                output["is_slave"] = True

                for row in results:
                    channel_name = row.get("Channel_Name", "")
                    io_running = row.get("Slave_IO_Running") or row.get(
                        "Replica_IO_Running"
                    )
                    sql_running = row.get("Slave_SQL_Running") or row.get(
                        "Replica_SQL_Running"
                    )

                    slave_info = {
                        "channel_name": channel_name,
                        "master_host": row.get("Master_Host") or row.get(
                            "Source_Host"
                        ),
                        "master_port": row.get("Master_Port") or row.get(
                            "Source_Port"
                        ),
                        "master_user": row.get("Master_User") or row.get(
                            "Source_User"
                        ),
                        "io_running": io_running,
                        "sql_running": sql_running,
                        "seconds_behind_master": row.get(
                            "Seconds_Behind_Master"
                        ) or row.get("Seconds_Behind_Source"),
                        "last_io_error": row.get("Last_IO_Error"),
                        "last_sql_error": row.get("Last_SQL_Error"),
                        "relay_log_file": row.get("Relay_Log_File"),
                        "relay_log_pos": row.get("Relay_Log_Pos"),
                        "master_log_file": row.get("Master_Log_File") or row.get(
                            "Source_Log_File"
                        ),
                        "read_master_log_pos": row.get(
                            "Read_Master_Log_Pos"
                        ) or row.get("Read_Source_Log_Pos"),
                        "exec_master_log_pos": row.get(
                            "Exec_Master_Log_Pos"
                        ) or row.get("Exec_Source_Log_Pos"),
                        "gtid_executed": row.get("Executed_Gtid_Set"),
                        "auto_position": row.get("Auto_Position")
                    }

                    # Check for issues
                    if io_running != "Yes":
                        output["issues"].append(
                            f"Channel '{channel_name}': Replica IO thread not running"
                        )
                        if row.get("Last_IO_Error"):
                            output["issues"].append(
                                f"IO Error: {row.get('Last_IO_Error')}"
                            )

                    if sql_running != "Yes":
                        output["issues"].append(
                            f"Channel '{channel_name}': Replica SQL thread not running"
                        )
                        if row.get("Last_SQL_Error"):
                            output["issues"].append(
                                f"SQL Error: {row.get('Last_SQL_Error')}"
                            )

                    # Check lag
                    lag = slave_info["seconds_behind_master"]
                    if lag is not None and lag > 60:
                        output["issues"].append(
                            f"Channel '{channel_name}': Replication lag is "
                            f"{lag} seconds"
                        )

                    output["slave_status"].append(slave_info)

        except Exception:
            # Not a slave or no privileges
            pass

    async def _get_replication_variables(
        self,
        variables: dict,
        output: dict
    ) -> None:
        """Get replication-related variables."""

        repl_vars = {}

        # Key replication variables
        var_names = [
            "server_id",
            "server_uuid",
            "log_bin",
            "binlog_format",
            "binlog_row_image",
            "gtid_mode",
            "enforce_gtid_consistency",
            "binlog_expire_logs_seconds",
            "expire_logs_days",
            "max_binlog_size",
            "sync_binlog",
            "relay_log_purge",
            "relay_log_recovery",
            "slave_parallel_workers",
            "slave_parallel_type",
            "replica_parallel_workers",
            "replica_parallel_type",
            "read_only",
            "super_read_only",
            "log_slave_updates",
            "log_replica_updates"
        ]

        for var in var_names:
            if var in variables:
                repl_vars[var] = variables[var]

        output["replication_variables"] = repl_vars

        # Check for common issues
        if repl_vars.get("server_id") == "0":
            output["issues"].append("server_id is 0 - replication requires unique ID")
            output["recommendations"].append(
                "Set server_id to a unique non-zero value"
            )

        if repl_vars.get("gtid_mode") == "OFF" and output["is_slave"]:
            output["recommendations"].append(
                "Consider enabling GTID mode for easier replication management"
            )

        sync_binlog = repl_vars.get("sync_binlog", "1")
        if sync_binlog == "0":
            output["issues"].append("sync_binlog=0 may cause data loss on crash")
            output["recommendations"].append(
                "Set sync_binlog=1 for crash-safe replication"
            )

    async def _check_semisync(
        self,
        variables: dict,
        status: dict,
        output: dict
    ) -> None:
        """Check semi-synchronous replication status."""

        semisync = {}

        # Check master semi-sync
        master_enabled = variables.get("rpl_semi_sync_master_enabled") or \
                        variables.get("rpl_semi_sync_source_enabled")

        if master_enabled:
            semisync["master_enabled"] = master_enabled == "ON"
            semisync["master_timeout"] = variables.get(
                "rpl_semi_sync_master_timeout"
            ) or variables.get("rpl_semi_sync_source_timeout")

            # Semi-sync status
            semisync["master_clients"] = status.get(
                "Rpl_semi_sync_master_clients"
            ) or status.get("Rpl_semi_sync_source_clients")
            semisync["master_status"] = status.get(
                "Rpl_semi_sync_master_status"
            ) or status.get("Rpl_semi_sync_source_status")

        # Check slave semi-sync
        slave_enabled = variables.get("rpl_semi_sync_slave_enabled") or \
                       variables.get("rpl_semi_sync_replica_enabled")

        if slave_enabled:
            semisync["slave_enabled"] = slave_enabled == "ON"
            semisync["slave_status"] = status.get(
                "Rpl_semi_sync_slave_status"
            ) or status.get("Rpl_semi_sync_replica_status")

        if semisync:
            output["semisync_replication"] = semisync

    def _generate_recommendations(self, output: dict) -> None:
        """Generate replication-related recommendations."""

        if output["is_master"]:
            # Master recommendations
            binlog_format = output["replication_variables"].get("binlog_format")
            if binlog_format == "STATEMENT":
                output["recommendations"].append(
                    "Consider using binlog_format=ROW for safer replication"
                )

            expire_days = output["replication_variables"].get("expire_logs_days")
            expire_seconds = output["replication_variables"].get(
                "binlog_expire_logs_seconds"
            )

            if not expire_days and not expire_seconds:
                output["recommendations"].append(
                    "Binary log expiration not set - logs may accumulate. "
                    "Set binlog_expire_logs_seconds or expire_logs_days."
                )

        if output["is_slave"]:
            # Slave recommendations
            for slave in output["slave_status"]:
                if slave.get("auto_position") != 1:
                    output["recommendations"].append(
                        f"Channel '{slave.get('channel_name', '')}': "
                        "Consider enabling MASTER_AUTO_POSITION=1 for GTID-based "
                        "replication for easier failover"
                    )

            parallel_workers = output["replication_variables"].get(
                "slave_parallel_workers"
            ) or output["replication_variables"].get("replica_parallel_workers")

            if parallel_workers and int(parallel_workers) == 0:
                output["recommendations"].append(
                    "Consider enabling parallel replication with "
                    "replica_parallel_workers > 0 for faster apply"
                )


class GaleraClusterToolHandler(ToolHandler):
    """Tool handler for Galera cluster status."""

    name = "get_galera_status"
    title = "Galera Cluster Status"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get Galera cluster status for MariaDB/Percona XtraDB Cluster.

Analyzes:
- Cluster membership and state
- Node status (Primary, Donor, Joiner)
- Flow control status
- Replication health metrics
- Certification and write-set conflicts

Only applicable to Galera-enabled MySQL variants."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            output = {
                "is_galera": False,
                "cluster_status": {},
                "node_status": {},
                "replication_health": {},
                "flow_control": {},
                "issues": [],
                "recommendations": []
            }

            status = await self.sql_driver.get_server_status("wsrep%")

            # Check if Galera is enabled
            wsrep_on = status.get("wsrep_on") or status.get("wsrep_ready")
            if not wsrep_on or wsrep_on == "OFF":
                output["message"] = "Galera replication is not enabled"
                return self.format_json_result(output)

            output["is_galera"] = True

            # Cluster status
            output["cluster_status"] = {
                "cluster_name": status.get("wsrep_cluster_name"),
                "cluster_size": int(status.get("wsrep_cluster_size", 0)),
                "cluster_state_uuid": status.get("wsrep_cluster_state_uuid"),
                "cluster_status": status.get("wsrep_cluster_status"),
                "cluster_conf_id": status.get("wsrep_cluster_conf_id")
            }

            # Check cluster status
            cluster_status = status.get("wsrep_cluster_status")
            if cluster_status != "Primary":
                output["issues"].append(
                    f"Cluster status is '{cluster_status}' (expected 'Primary')"
                )

            cluster_size = int(status.get("wsrep_cluster_size", 0))
            if cluster_size < 3:
                output["issues"].append(
                    f"Cluster size is {cluster_size} (recommended minimum: 3)"
                )

            # Node status
            output["node_status"] = {
                "node_name": status.get("wsrep_node_name"),
                "node_address": status.get("wsrep_node_address"),
                "local_state": int(status.get("wsrep_local_state", 0)),
                "local_state_comment": status.get("wsrep_local_state_comment"),
                "ready": status.get("wsrep_ready"),
                "connected": status.get("wsrep_connected"),
                "provider_version": status.get("wsrep_provider_version")
            }

            # Check node state (4 = Synced)
            local_state = int(status.get("wsrep_local_state", 0))
            if local_state != 4:
                output["issues"].append(
                    f"Node state is '{status.get('wsrep_local_state_comment')}' "
                    "(expected 'Synced')"
                )

            if status.get("wsrep_ready") != "ON":
                output["issues"].append("Node is not ready for queries")

            if status.get("wsrep_connected") != "ON":
                output["issues"].append("Node is not connected to cluster")

            # Replication health
            recv_queue = int(status.get("wsrep_local_recv_queue", 0))
            send_queue = int(status.get("wsrep_local_send_queue", 0))
            cert_failures = int(status.get("wsrep_local_cert_failures", 0))
            bf_aborts = int(status.get("wsrep_local_bf_aborts", 0))

            output["replication_health"] = {
                "local_recv_queue": recv_queue,
                "local_recv_queue_avg": float(
                    status.get("wsrep_local_recv_queue_avg", 0)
                ),
                "local_send_queue": send_queue,
                "local_send_queue_avg": float(
                    status.get("wsrep_local_send_queue_avg", 0)
                ),
                "cert_deps_distance": float(
                    status.get("wsrep_cert_deps_distance", 0)
                ),
                "apply_window": float(
                    status.get("wsrep_apply_window", 0)
                ),
                "commit_window": float(
                    status.get("wsrep_commit_window", 0)
                ),
                "local_cert_failures": cert_failures,
                "local_bf_aborts": bf_aborts,
                "replicated_bytes": int(status.get("wsrep_replicated_bytes", 0)),
                "received_bytes": int(status.get("wsrep_received_bytes", 0))
            }

            if recv_queue > 10:
                output["issues"].append(
                    f"High receive queue ({recv_queue}) - node may be slow to apply"
                )

            if cert_failures > 0:
                output["issues"].append(
                    f"{cert_failures} certification failures detected - "
                    "check for conflicting writes"
                )

            # Flow control
            output["flow_control"] = {
                "paused": float(status.get("wsrep_flow_control_paused", 0)),
                "paused_ns": int(status.get("wsrep_flow_control_paused_ns", 0)),
                "sent": int(status.get("wsrep_flow_control_sent", 0)),
                "recv": int(status.get("wsrep_flow_control_recv", 0))
            }

            paused_pct = float(status.get("wsrep_flow_control_paused", 0))
            if paused_pct > 0.1:  # More than 10% paused
                output["issues"].append(
                    f"Flow control paused {paused_pct * 100:.1f}% of time - "
                    "indicates slow node in cluster"
                )
                output["recommendations"].append(
                    "Investigate slow node or increase wsrep_slave_threads"
                )

            # Recommendations
            if not output["issues"]:
                output["recommendations"].append(
                    "Galera cluster appears healthy"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class GroupReplicationToolHandler(ToolHandler):
    """Tool handler for MySQL Group Replication status."""

    name = "get_group_replication_status"
    title = "Group Replication Status"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get MySQL Group Replication status.

Analyzes:
- Group membership and state
- Member roles (PRIMARY/SECONDARY)
- Replication channels
- Transaction certification
- Flow control

Only applicable to MySQL with Group Replication enabled."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            output = {
                "is_group_replication": False,
                "group_status": {},
                "members": [],
                "local_member": {},
                "issues": [],
                "recommendations": []
            }

            variables = await self.sql_driver.get_server_variables(
                "group_replication%"
            )

            # Check if Group Replication is enabled
            gr_enabled = variables.get("group_replication_group_name")
            if not gr_enabled:
                output["message"] = "Group Replication is not configured"
                return self.format_json_result(output)

            # Get member status
            try:
                members_query = """
                    SELECT
                        CHANNEL_NAME,
                        MEMBER_ID,
                        MEMBER_HOST,
                        MEMBER_PORT,
                        MEMBER_STATE,
                        MEMBER_ROLE,
                        MEMBER_VERSION
                    FROM performance_schema.replication_group_members
                """
                members = await self.sql_driver.execute_query(members_query)

                if not members:
                    output["message"] = "Group Replication not active"
                    return self.format_json_result(output)

                output["is_group_replication"] = True

                for member in members:
                    member_info = {
                        "member_id": member.get("MEMBER_ID"),
                        "host": member.get("MEMBER_HOST"),
                        "port": member.get("MEMBER_PORT"),
                        "state": member.get("MEMBER_STATE"),
                        "role": member.get("MEMBER_ROLE"),
                        "version": member.get("MEMBER_VERSION")
                    }
                    output["members"].append(member_info)

                    if member.get("MEMBER_STATE") != "ONLINE":
                        output["issues"].append(
                            f"Member {member.get('MEMBER_HOST')}:{member.get('MEMBER_PORT')} "
                            f"is {member.get('MEMBER_STATE')}"
                        )

            except Exception:
                output["message"] = "Group Replication tables not available"
                return self.format_json_result(output)

            # Group status
            output["group_status"] = {
                "group_name": variables.get("group_replication_group_name"),
                "single_primary_mode": variables.get(
                    "group_replication_single_primary_mode"
                ),
                "auto_rejoin_tries": variables.get(
                    "group_replication_autorejoin_tries"
                ),
                "member_count": len(members)
            }

            # Check quorum
            online_members = len([
                m for m in output["members"]
                if m["state"] == "ONLINE"
            ])

            if online_members < (len(output["members"]) // 2 + 1):
                output["issues"].append(
                    f"Potential quorum loss: only {online_members} of "
                    f"{len(output['members'])} members online"
                )

            # Get local member info
            try:
                local_query = """
                    SELECT
                        MEMBER_ID,
                        COUNT_TRANSACTIONS_IN_QUEUE,
                        COUNT_TRANSACTIONS_CHECKED,
                        COUNT_CONFLICTS_DETECTED,
                        COUNT_TRANSACTIONS_ROWS_VALIDATING,
                        TRANSACTIONS_COMMITTED_ALL_MEMBERS,
                        LAST_CONFLICT_FREE_TRANSACTION
                    FROM performance_schema.replication_group_member_stats
                    WHERE MEMBER_ID = @@server_uuid
                """
                local_stats = await self.sql_driver.execute_query(local_query)

                if local_stats:
                    stat = local_stats[0]
                    output["local_member"] = {
                        "transactions_in_queue": stat.get(
                            "COUNT_TRANSACTIONS_IN_QUEUE"
                        ),
                        "transactions_checked": stat.get(
                            "COUNT_TRANSACTIONS_CHECKED"
                        ),
                        "conflicts_detected": stat.get("COUNT_CONFLICTS_DETECTED"),
                        "rows_validating": stat.get(
                            "COUNT_TRANSACTIONS_ROWS_VALIDATING"
                        )
                    }

                    conflicts = stat.get("COUNT_CONFLICTS_DETECTED") or 0
                    if conflicts > 0:
                        output["issues"].append(
                            f"{conflicts} certification conflicts detected"
                        )

                    queue_size = stat.get("COUNT_TRANSACTIONS_IN_QUEUE") or 0
                    if queue_size > 100:
                        output["issues"].append(
                            f"High transaction queue ({queue_size})"
                        )

            except Exception:
                pass

            # Recommendations
            if len(output["members"]) < 3:
                output["recommendations"].append(
                    "Consider adding more members for better fault tolerance "
                    "(minimum 3 recommended)"
                )

            if not output["issues"]:
                output["recommendations"].append(
                    "Group Replication appears healthy"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
