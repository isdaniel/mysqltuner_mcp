"""
Security analysis tool handlers for MySQL.

Includes tools for analyzing security:
- User account security checks
- Password policies
- Privilege analysis
- SSL/TLS configuration
- Anonymous users and weak accounts

Based on MySQLTuner's security recommendations patterns.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class SecurityAnalysisToolHandler(ToolHandler):
    """Tool handler for MySQL security analysis."""

    name = "analyze_security"
    title = "Security Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Perform comprehensive MySQL security analysis.

Checks:
- Anonymous user accounts
- Users without passwords
- Users with weak password policies
- Root account security
- Password validation plugin status
- SSL/TLS configuration
- Host-based access patterns
- Dangerous privileges (SUPER, FILE, GRANT)

Based on MySQLTuner's security_recommendations() function."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_user_list": {
                        "type": "boolean",
                        "description": "Include full user list in output",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_users = arguments.get("include_user_list", False)

            output = {
                "security_score": 0,
                "checks": {},
                "ssl_status": {},
                "password_policy": {},
                "issues": [],
                "recommendations": [],
                "users": []
            }

            total_checks = 0
            passed_checks = 0

            # Check for anonymous users
            total_checks += 1
            anon_check = await self._check_anonymous_users(output)
            if anon_check:
                passed_checks += 1

            # Check for users without passwords
            total_checks += 1
            no_pass_check = await self._check_users_without_password(output)
            if no_pass_check:
                passed_checks += 1

            # Check root account
            total_checks += 1
            root_check = await self._check_root_account(output)
            if root_check:
                passed_checks += 1

            # Check password validation
            total_checks += 1
            pw_policy_check = await self._check_password_validation(output)
            if pw_policy_check:
                passed_checks += 1

            # Check SSL/TLS
            total_checks += 1
            ssl_check = await self._check_ssl_status(output)
            if ssl_check:
                passed_checks += 1

            # Check dangerous privileges
            total_checks += 1
            priv_check = await self._check_dangerous_privileges(output)
            if priv_check:
                passed_checks += 1

            # Check wildcard hosts
            total_checks += 1
            host_check = await self._check_wildcard_hosts(output)
            if host_check:
                passed_checks += 1

            # Check test databases
            total_checks += 1
            test_db_check = await self._check_test_databases(output)
            if test_db_check:
                passed_checks += 1

            # Calculate security score
            output["security_score"] = round((passed_checks / total_checks) * 100, 1)
            output["checks"]["total"] = total_checks
            output["checks"]["passed"] = passed_checks
            output["checks"]["failed"] = total_checks - passed_checks

            # Get user list if requested
            if include_users:
                await self._get_user_list(output)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _check_anonymous_users(self, output: dict) -> bool:
        """Check for anonymous user accounts."""
        try:
            query = """
                SELECT User, Host
                FROM mysql.user
                WHERE User = ''
            """
            results = await self.sql_driver.execute_query(query)

            output["checks"]["anonymous_users"] = {
                "passed": len(results) == 0,
                "count": len(results)
            }

            if results:
                output["issues"].append(
                    f"Found {len(results)} anonymous user account(s)"
                )
                output["recommendations"].append(
                    "Remove anonymous users: DROP USER ''@'host'"
                )
                for row in results:
                    output["issues"].append(
                        f"Anonymous user: ''@'{row.get('Host')}'"
                    )
                return False

            return True

        except Exception:
            output["checks"]["anonymous_users"] = {"passed": None, "error": True}
            return False

    async def _check_users_without_password(self, output: dict) -> bool:
        """Check for users without passwords."""
        try:
            # MySQL 5.7+ uses authentication_string
            query = """
                SELECT User, Host, plugin
                FROM mysql.user
                WHERE (authentication_string = '' OR authentication_string IS NULL)
                    AND plugin NOT IN (
                        'auth_socket', 'unix_socket',
                        'mysql_native_password', 'caching_sha2_password'
                    )
                    AND User != ''
            """
            try:
                results = await self.sql_driver.execute_query(query)
            except Exception:
                # Try alternative for older versions
                query = """
                    SELECT User, Host
                    FROM mysql.user
                    WHERE Password = '' AND User != ''
                """
                results = await self.sql_driver.execute_query(query)

            # Also check for users with empty password via plugin
            plugin_query = """
                SELECT User, Host, plugin
                FROM mysql.user
                WHERE User != ''
                    AND (authentication_string = '' OR authentication_string IS NULL)
                    AND plugin IN ('mysql_native_password', 'caching_sha2_password')
            """
            try:
                plugin_results = await self.sql_driver.execute_query(plugin_query)
                results.extend(plugin_results)
            except Exception:
                pass

            output["checks"]["users_without_password"] = {
                "passed": len(results) == 0,
                "count": len(results)
            }

            if results:
                output["issues"].append(
                    f"Found {len(results)} user(s) without password"
                )
                output["recommendations"].append(
                    "Set passwords for all accounts or remove unused accounts"
                )
                for row in results:
                    output["issues"].append(
                        f"User without password: '{row.get('User')}'@'{row.get('Host')}'"
                    )
                return False

            return True

        except Exception:
            output["checks"]["users_without_password"] = {"passed": None, "error": True}
            return False

    async def _check_root_account(self, output: dict) -> bool:
        """Check root account security."""
        issues_found = False

        try:
            # Check if root can connect from any host
            query = """
                SELECT User, Host
                FROM mysql.user
                WHERE User = 'root' AND Host NOT IN ('localhost', '127.0.0.1', '::1')
            """
            results = await self.sql_driver.execute_query(query)

            output["checks"]["root_remote_access"] = {
                "passed": len(results) == 0,
                "count": len(results)
            }

            if results:
                issues_found = True
                output["issues"].append(
                    "Root account allows remote access"
                )
                output["recommendations"].append(
                    "Restrict root to localhost only. Create separate admin accounts "
                    "for remote access if needed."
                )

            # Check if root exists at all (some setups rename it)
            root_check = """
                SELECT COUNT(*) as cnt FROM mysql.user WHERE User = 'root'
            """
            root_count = await self.sql_driver.execute_scalar(root_check)

            if root_count and root_count > 0:
                output["checks"]["root_exists"] = {"exists": True}
                output["recommendations"].append(
                    "Consider renaming the root account for security through obscurity"
                )

            return not issues_found

        except Exception:
            output["checks"]["root_account"] = {"passed": None, "error": True}
            return False

    async def _check_password_validation(self, output: dict) -> bool:
        """Check password validation plugin status."""
        try:
            variables = await self.sql_driver.get_server_variables()

            # Check for validate_password component/plugin
            validate_policy = variables.get("validate_password.policy") or \
                            variables.get("validate_password_policy")

            validate_length = variables.get("validate_password.length") or \
                            variables.get("validate_password_length")

            if validate_policy:
                output["password_policy"] = {
                    "enabled": True,
                    "policy": validate_policy,
                    "min_length": validate_length,
                    "mixed_case": variables.get(
                        "validate_password.mixed_case_count"
                    ) or variables.get("validate_password_mixed_case_count"),
                    "number_count": variables.get(
                        "validate_password.number_count"
                    ) or variables.get("validate_password_number_count"),
                    "special_char": variables.get(
                        "validate_password.special_char_count"
                    ) or variables.get("validate_password_special_char_count")
                }

                output["checks"]["password_validation"] = {"passed": True}
                return True
            else:
                output["password_policy"] = {"enabled": False}
                output["issues"].append(
                    "Password validation plugin is not enabled"
                )
                output["recommendations"].append(
                    "Enable validate_password component: "
                    "INSTALL COMPONENT 'file://component_validate_password'"
                )
                output["checks"]["password_validation"] = {"passed": False}
                return False

        except Exception:
            output["checks"]["password_validation"] = {"passed": None, "error": True}
            return False

    async def _check_ssl_status(self, output: dict) -> bool:
        """Check SSL/TLS configuration."""
        try:
            variables = await self.sql_driver.get_server_variables()
            status = await self.sql_driver.get_server_status()

            have_ssl = variables.get("have_ssl")
            have_openssl = variables.get("have_openssl")
            ssl_cipher = variables.get("ssl_cipher")
            require_secure_transport = variables.get("require_secure_transport")

            output["ssl_status"] = {
                "have_ssl": have_ssl,
                "have_openssl": have_openssl,
                "ssl_cipher": ssl_cipher,
                "require_secure_transport": require_secure_transport,
                "ssl_ca": variables.get("ssl_ca"),
                "ssl_cert": variables.get("ssl_cert"),
                "ssl_key": variables.get("ssl_key"),
                "tls_version": variables.get("tls_version")
            }

            passed = True

            if have_ssl != "YES":
                passed = False
                output["issues"].append("SSL is not enabled")
                output["recommendations"].append(
                    "Enable SSL for encrypted connections"
                )
            else:
                # Check SSL usage
                ssl_accepts = int(status.get("Ssl_accepts", 0))
                ssl_finished = int(status.get("Ssl_finished_accepts", 0))

                output["ssl_status"]["ssl_connections"] = {
                    "accepts": ssl_accepts,
                    "finished": ssl_finished
                }

            if require_secure_transport != "ON":
                output["recommendations"].append(
                    "Consider setting require_secure_transport=ON to enforce SSL"
                )

            # Check for weak TLS versions
            tls_version = variables.get("tls_version", "")
            if "TLSv1" in tls_version and "TLSv1.2" not in tls_version:
                output["issues"].append(
                    f"Weak TLS version enabled: {tls_version}"
                )
                output["recommendations"].append(
                    "Disable TLSv1 and TLSv1.1, use TLSv1.2 or higher"
                )

            output["checks"]["ssl_enabled"] = {"passed": passed}
            return passed

        except Exception:
            output["checks"]["ssl_enabled"] = {"passed": None, "error": True}
            return False

    async def _check_dangerous_privileges(self, output: dict) -> bool:
        """Check for users with dangerous privileges."""
        try:
            # Check for SUPER, FILE, PROCESS, SHUTDOWN privileges
            query = """
                SELECT User, Host,
                    Super_priv,
                    File_priv,
                    Process_priv,
                    Shutdown_priv,
                    Grant_priv
                FROM mysql.user
                WHERE User != 'root'
                    AND User != 'mysql.sys'
                    AND User != 'mysql.session'
                    AND User != 'mysql.infoschema'
                    AND (Super_priv = 'Y'
                        OR File_priv = 'Y'
                        OR Process_priv = 'Y'
                        OR Shutdown_priv = 'Y')
            """
            results = await self.sql_driver.execute_query(query)

            output["checks"]["dangerous_privileges"] = {
                "passed": len(results) == 0,
                "count": len(results)
            }

            if results:
                output["issues"].append(
                    f"Found {len(results)} non-root user(s) with dangerous privileges"
                )
                for row in results:
                    privs = []
                    if row.get("Super_priv") == "Y":
                        privs.append("SUPER")
                    if row.get("File_priv") == "Y":
                        privs.append("FILE")
                    if row.get("Process_priv") == "Y":
                        privs.append("PROCESS")
                    if row.get("Shutdown_priv") == "Y":
                        privs.append("SHUTDOWN")

                    output["issues"].append(
                        f"User '{row.get('User')}'@'{row.get('Host')}' has: "
                        f"{', '.join(privs)}"
                    )

                output["recommendations"].append(
                    "Review and revoke unnecessary SUPER, FILE, PROCESS, "
                    "SHUTDOWN privileges"
                )
                return False

            return True

        except Exception:
            output["checks"]["dangerous_privileges"] = {"passed": None, "error": True}
            return False

    async def _check_wildcard_hosts(self, output: dict) -> bool:
        """Check for users with wildcard host patterns."""
        try:
            query = """
                SELECT User, Host
                FROM mysql.user
                WHERE Host = '%'
                    AND User != ''
                    AND User NOT IN (
                        'mysql.sys', 'mysql.session', 'mysql.infoschema'
                    )
            """
            results = await self.sql_driver.execute_query(query)

            output["checks"]["wildcard_hosts"] = {
                "passed": len(results) == 0,
                "count": len(results)
            }

            if results:
                output["issues"].append(
                    f"Found {len(results)} user(s) with wildcard host '%'"
                )
                for row in results:
                    output["issues"].append(
                        f"Wildcard host: '{row.get('User')}'@'%'"
                    )
                output["recommendations"].append(
                    "Restrict user accounts to specific hosts/IPs instead of '%'"
                )
                return False

            return True

        except Exception:
            output["checks"]["wildcard_hosts"] = {"passed": None, "error": True}
            return False

    async def _check_test_databases(self, output: dict) -> bool:
        """Check for test databases."""
        try:
            query = """
                SELECT SCHEMA_NAME
                FROM information_schema.SCHEMATA
                WHERE SCHEMA_NAME LIKE 'test%'
            """
            results = await self.sql_driver.execute_query(query)

            output["checks"]["test_databases"] = {
                "passed": len(results) == 0,
                "count": len(results)
            }

            if results:
                dbs = [row.get("SCHEMA_NAME") for row in results]
                output["issues"].append(
                    f"Found test database(s): {', '.join(dbs)}"
                )
                output["recommendations"].append(
                    "Remove test databases in production: DROP DATABASE test"
                )
                return False

            return True

        except Exception:
            output["checks"]["test_databases"] = {"passed": None, "error": True}
            return False

    async def _get_user_list(self, output: dict) -> None:
        """Get full user list."""
        try:
            query = """
                SELECT
                    User,
                    Host,
                    plugin,
                    account_locked,
                    password_expired,
                    password_lifetime,
                    password_last_changed,
                    max_connections,
                    max_user_connections
                FROM mysql.user
                ORDER BY User, Host
            """
            results = await self.sql_driver.execute_query(query)

            output["users"] = [
                {
                    "user": row.get("User"),
                    "host": row.get("Host"),
                    "plugin": row.get("plugin"),
                    "account_locked": row.get("account_locked"),
                    "password_expired": row.get("password_expired"),
                    "password_lifetime": row.get("password_lifetime"),
                    "password_last_changed": str(row.get("password_last_changed")),
                    "max_connections": row.get("max_connections"),
                    "max_user_connections": row.get("max_user_connections")
                }
                for row in results
            ]

        except Exception:
            output["users"] = []


class UserPrivilegesToolHandler(ToolHandler):
    """Tool handler for user privileges analysis."""

    name = "analyze_user_privileges"
    title = "User Privileges Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze privileges for a specific user or all users.

Shows:
- Global privileges
- Database-level privileges
- Table-level privileges
- Column-level privileges
- Routine privileges

Helps identify excessive or missing privileges."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "username": {
                        "type": "string",
                        "description": "Username to analyze (omit for all users)"
                    },
                    "hostname": {
                        "type": "string",
                        "description": "Host pattern for the user",
                        "default": "%"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            username = arguments.get("username")
            hostname = arguments.get("hostname", "%")

            output = {
                "users": [],
                "summary": {},
                "recommendations": []
            }

            if username:
                # Analyze specific user
                user_privs = await self._get_user_privileges(username, hostname)
                output["users"].append(user_privs)
            else:
                # Get list of all users and their privilege summary
                users_query = """
                    SELECT DISTINCT User, Host
                    FROM mysql.user
                    WHERE User != ''
                    ORDER BY User, Host
                    LIMIT 100
                """
                users = await self.sql_driver.execute_query(users_query)

                for user in users:
                    user_privs = await self._get_user_privileges(
                        user.get("User"),
                        user.get("Host")
                    )
                    output["users"].append(user_privs)

            # Generate summary
            total_users = len(output["users"])
            super_users = len([
                u for u in output["users"]
                if u.get("global_privileges", {}).get("Super_priv") == "Y"
            ])
            grant_users = len([
                u for u in output["users"]
                if u.get("global_privileges", {}).get("Grant_priv") == "Y"
            ])

            output["summary"] = {
                "total_users_analyzed": total_users,
                "users_with_super": super_users,
                "users_with_grant": grant_users
            }

            if super_users > 2:
                output["recommendations"].append(
                    f"{super_users} users have SUPER privilege - review necessity"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _get_user_privileges(self, username: str, hostname: str) -> dict:
        """Get privileges for a specific user."""
        user_privs = {
            "user": username,
            "host": hostname,
            "global_privileges": {},
            "database_privileges": [],
            "table_privileges": [],
            "column_privileges": [],
            "routine_privileges": []
        }

        # Global privileges
        try:
            global_query = f"""
                SELECT *
                FROM mysql.user
                WHERE User = '{username}' AND Host = '{hostname}'
            """
            global_result = await self.sql_driver.execute_query(global_query)

            if global_result:
                row = global_result[0]
                # Extract privilege columns (those ending in _priv)
                user_privs["global_privileges"] = {
                    k: v for k, v in row.items()
                    if k.endswith("_priv") and v == "Y"
                }
        except Exception:
            pass

        # Database privileges
        try:
            db_query = f"""
                SELECT Db, Select_priv, Insert_priv, Update_priv, Delete_priv,
                       Create_priv, Drop_priv, Grant_priv, Index_priv, Alter_priv,
                       Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,
                       Show_view_priv, Create_routine_priv, Alter_routine_priv,
                       Execute_priv, Event_priv, Trigger_priv
                FROM mysql.db
                WHERE User = '{username}' AND Host = '{hostname}'
            """
            db_results = await self.sql_driver.execute_query(db_query)

            for row in db_results:
                privs = {k: v for k, v in row.items() if v == "Y" and k != "Db"}
                user_privs["database_privileges"].append({
                    "database": row.get("Db"),
                    "privileges": list(privs.keys())
                })
        except Exception:
            pass

        # Table privileges
        try:
            table_query = f"""
                SELECT Db, Table_name, Table_priv, Column_priv
                FROM mysql.tables_priv
                WHERE User = '{username}' AND Host = '{hostname}'
            """
            table_results = await self.sql_driver.execute_query(table_query)

            for row in table_results:
                user_privs["table_privileges"].append({
                    "database": row.get("Db"),
                    "table": row.get("Table_name"),
                    "table_priv": row.get("Table_priv"),
                    "column_priv": row.get("Column_priv")
                })
        except Exception:
            pass

        return user_privs


class AuditLogToolHandler(ToolHandler):
    """Tool handler for MySQL audit log analysis."""

    name = "check_audit_log"
    title = "Audit Log Checker"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Check MySQL audit log configuration and status.

Analyzes:
- Audit plugin status
- Audit log configuration
- Recent audit events (if accessible)
- Compliance recommendations

Supports MySQL Enterprise Audit, MariaDB Audit Plugin,
and Percona Audit Log Plugin."""

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
                "audit_enabled": False,
                "audit_plugin": None,
                "configuration": {},
                "recommendations": []
            }

            variables = await self.sql_driver.get_server_variables()

            # Check for MySQL Enterprise Audit
            audit_log = variables.get("audit_log_file")
            if audit_log:
                output["audit_enabled"] = True
                output["audit_plugin"] = "MySQL Enterprise Audit"
                output["configuration"] = {
                    "audit_log_file": audit_log,
                    "audit_log_format": variables.get("audit_log_format"),
                    "audit_log_policy": variables.get("audit_log_policy"),
                    "audit_log_rotate_on_size": variables.get(
                        "audit_log_rotate_on_size"
                    ),
                    "audit_log_buffer_size": variables.get("audit_log_buffer_size")
                }

            # Check for MariaDB Audit Plugin
            server_audit = variables.get("server_audit_logging")
            if server_audit:
                output["audit_enabled"] = server_audit == "ON"
                output["audit_plugin"] = "MariaDB Server Audit"
                output["configuration"] = {
                    "server_audit_logging": server_audit,
                    "server_audit_file_path": variables.get("server_audit_file_path"),
                    "server_audit_events": variables.get("server_audit_events"),
                    "server_audit_incl_users": variables.get(
                        "server_audit_incl_users"
                    ),
                    "server_audit_excl_users": variables.get(
                        "server_audit_excl_users"
                    )
                }

            # Check for Percona Audit Log
            audit_log_policy = variables.get("audit_log_policy")
            if audit_log_policy and not audit_log:
                output["audit_enabled"] = True
                output["audit_plugin"] = "Percona Audit Log"
                output["configuration"]["audit_log_policy"] = audit_log_policy

            # Check for loaded plugins
            try:
                plugins_query = """
                    SELECT PLUGIN_NAME, PLUGIN_STATUS
                    FROM information_schema.PLUGINS
                    WHERE PLUGIN_NAME LIKE '%audit%'
                """
                plugins = await self.sql_driver.execute_query(plugins_query)
                if plugins:
                    output["installed_plugins"] = [
                        {
                            "name": p.get("PLUGIN_NAME"),
                            "status": p.get("PLUGIN_STATUS")
                        }
                        for p in plugins
                    ]
            except Exception:
                pass

            # Recommendations
            if not output["audit_enabled"]:
                output["recommendations"].append(
                    "No audit logging detected. Consider enabling audit logging "
                    "for compliance and security monitoring."
                )
                output["recommendations"].append(
                    "Options: MySQL Enterprise Audit (commercial), "
                    "MariaDB Audit Plugin (free), or Percona Audit Log (free)"
                )
            else:
                output["recommendations"].append(
                    f"{output['audit_plugin']} is enabled"
                )
                output["recommendations"].append(
                    "Ensure audit logs are being monitored and regularly reviewed"
                )
                output["recommendations"].append(
                    "Configure log rotation to prevent disk space issues"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
