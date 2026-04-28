"""
Shared DuckDB file open (Windows path variants + ATTACH fallback).
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

ATTACH_ALIAS = "jds_books"


def normalize_db_path(db_arg: Path) -> Path:
    p = Path(os.path.normpath(str(Path(db_arg).expanduser())))
    if not p.is_absolute():
        p = Path(os.path.normpath(str((Path.cwd() / p).resolve(strict=False))))
    try:
        p = p.resolve(strict=False)
    except OSError:
        pass
    return p


def candidate_connect_strings(p: Path) -> list[str]:
    """Ordered path strings to try with duckdb.connect(...) and ATTACH."""
    s = os.path.normpath(str(p))
    out: list[str] = []
    seen: set[str] = set()

    def add(x: str) -> None:
        if x and x not in seen:
            seen.add(x)
            out.append(x)

    if os.name == "nt":
        add(s)
        add(s.replace("/", "\\"))
        # Win32 long path prefix for drive letters (often helps mapped / SUBST drives).
        if len(s) >= 2 and s[1] == ":" and not s.upper().startswith("\\\\?\\"):
            add("\\\\?\\" + s)
        add(Path(s).as_posix())
    else:
        add(Path(s).as_posix())
        add(s)
    return out


def _attach_sql_path(cand: str) -> str:
    """Escape a path for use inside single quotes in ATTACH; use forward slashes."""
    return cand.replace("\\", "/").replace("'", "''")


def connect_duckdb_database(
    db_path: Path, read_only: bool, sql_table_basename: str
) -> tuple[duckdb.DuckDBPyConnection, str, str, bool]:
    """
    Returns (connection, manifest_path_posix, qualified_table_sql, used_attach_fallback).
    """
    p = normalize_db_path(Path(db_path))
    manifest_posix = p.as_posix()
    errors: list[str] = []
    ro = " (READ_ONLY)" if read_only else ""

    for cand in candidate_connect_strings(p):
        try:
            con = duckdb.connect(cand, read_only=read_only)
            con.execute("SELECT 1")
            return con, manifest_posix, sql_table_basename, False
        except Exception as e:
            errors.append(f"  connect({cand!r}): {e}")

    for cand in candidate_connect_strings(p):
        esc = _attach_sql_path(cand)
        try:
            con = duckdb.connect(":memory:")
            con.execute(f"ATTACH '{esc}' AS {ATTACH_ALIAS}{ro};")
            con.execute("SELECT 1")
            qualified = f"{ATTACH_ALIAS}.{sql_table_basename}"
            return con, manifest_posix, qualified, True
        except Exception as e:
            errors.append(f"  ATTACH('{esc}'): {e}")

    raise SystemExit(
        "DuckDB could not open the database file. Attempts:\n"
        + "\n".join(errors)
        + "\n\nWorkarounds:\n"
        + "  * Put the DB on a local NTFS path, e.g.\n"
        + '      --db "C:/Users/<you>/AppData/Local/job_reqs_book_matcher/jds_books.duckdb"\n'
        + "  * Or set JD_STAGING_DATA_DIR to a folder on a local drive.\n"
        + "  * Mapped drives (e.g. Z:) sometimes fail with certain DuckDB / Python builds; cloning the repo to C: avoids this.\n"
    )
