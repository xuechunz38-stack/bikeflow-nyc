"""B6: set up a fresh database the same way compose does (used by CI).

Sets the database timezone to America/New_York (compose does this with
`-c timezone=...`), then runs sql/schema.sql and sql/views.sql if present.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bikeflow.db import get_conn

SQL_FILES = ["sql/schema.sql", "sql/views.sql"]


def main() -> None:
    with get_conn() as conn:
        db = conn.info.dbname
        conn.execute(f"ALTER DATABASE {db} SET timezone TO 'America/New_York'")
    for name in SQL_FILES:
        path = ROOT / name
        if path.exists():
            with get_conn() as conn:
                conn.execute(path.read_text())
            print(f"ran {name}")


if __name__ == "__main__":
    main()
