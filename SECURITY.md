# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | Yes       |
| < 0.2   | No        |

## Reporting a Vulnerability

Please report security vulnerabilities by emailing **dog830228@gmail.com**.

Do **not** open a public GitHub issue for security reports.

Include:
- A description of the vulnerability and its impact.
- Reproduction steps or a proof-of-concept.
- The affected version (`pip show mysqltuner_mcp`).
- Any suggested mitigation.

You can expect:
- An acknowledgement within **3 business days**.
- A status update within **10 business days**.
- A coordinated disclosure timeline if the report is accepted.

## Scope

This policy covers the code under `src/mysqltuner_mcp/` published as the
`mysqltuner_mcp` PyPI package and the `dog830228/mysqltuner_mcp` Docker
image. Vulnerabilities in upstream dependencies should be reported to the
respective upstream projects; we will track and patch downstream.

## Out of Scope

- Issues that require an attacker to already have a valid `MYSQL_URI` with
  high privileges (the threat model assumes the operator controls the URI).
- Denial of service via large query results — the server returns whatever
  MySQL returns; size limits are the operator's responsibility.
- Bugs in `aiomysql`, `pymysql`, or `mcp` — report upstream.
