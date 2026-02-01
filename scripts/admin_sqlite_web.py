from pathlib import Path
import subprocess
import sys


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "stockbot.db"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "sqlite_web",
            str(db_path),
            "--host", 
            "0.0.0.0",
            "--port",
            "8081",
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
