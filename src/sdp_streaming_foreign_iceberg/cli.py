"""Capture a portable experiment record without starting Spark or accessing AWS."""

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import distributions
import json
from pathlib import Path
import platform
import subprocess
import tomllib


def command(*args):
    try:
        result = subprocess.run(args, capture_output=True, text=True, check=False)
        return {"exit_code": result.returncode, "output": (result.stdout + result.stderr).strip()}
    except FileNotFoundError:
        return {"exit_code": None, "output": "not installed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/example.toml"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/environment.json"))
    args = parser.parse_args()
    configuration = tomllib.loads(args.config.read_text())
    root = Path(__file__).resolve().parents[2]
    lock = root / "uv.lock"
    record = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "uv": command("uv", "--version"),
        "java": command("java", "-version"),
        "git_commit": command("git", "-C", str(root), "rev-parse", "HEAD"),
        "git_status": command("git", "-C", str(root), "status", "--porcelain"),
        "lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest() if lock.exists() else None,
        "packages": sorted(
            ({"name": d.metadata["Name"], "version": d.version} for d in distributions()),
            key=lambda p: p["name"].lower(),
        ),
        "configuration": configuration,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n")
    print(f"Environment record written to {args.output}")


if __name__ == "__main__":
    main()
