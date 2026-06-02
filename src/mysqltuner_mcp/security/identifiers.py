"""Safe SQL identifier quoting.

MySQL identifiers can be up to 64 characters and use the character set
[0-9a-zA-Z$_]. We intentionally reject everything outside that set rather
than try to escape backticks — escaping is error-prone and a whitelist is
sufficient for every identifier source in this codebase (schema names,
table names, index names from information_schema, user input).
"""

from __future__ import annotations

import re

IDENT_RE = re.compile(r"\A[A-Za-z0-9_$]{1,64}\Z")


def quote_ident(name: object) -> str:
    """Quote a SQL identifier with backticks.

    Raises:
        ValueError: if the input is not a string or contains anything
            outside the MySQL identifier character set. The exception
            message does NOT echo the input, to avoid reflecting a
            probing payload back to the caller.
    """
    if not isinstance(name, str) or not IDENT_RE.match(name):
        raise ValueError("Invalid SQL identifier")
    return f"`{name}`"
