#!/usr/bin/env python3
"""Apply sql/schema.sql to the configured DuckDB file (no Spark required)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from config.settings import DUCKDB_PATH, SQL_DIR  # noqa: E402


def main() -> None:
    schema = (SQL_DIR / "schema.sql").read_text()
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(schema)
    conn.commit()
    conn.close()
    print(f"Bootstrap OK — {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
