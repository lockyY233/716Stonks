from pathlib import Path
from sqlite_web import sqlite_web


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "stockbot.db"
    sqlite_web.run(
        db_path=str(db_path),
        host="127.0.0.1",
        port=8081,
    )


if __name__ == "__main__":
    main()
