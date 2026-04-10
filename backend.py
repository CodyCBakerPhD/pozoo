"""
Manages the sparse-checkout Git repository and all write / commit / push
operations.
"""

import json
import os
import subprocess
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from config import Config


def _run(cmd: list[str], cwd: str | None = None, check: bool = True):
    """Run a shell command and return the CompletedProcess."""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command {cmd} failed (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}"
        )
    return result


def ensure_repo() -> str:
    """
    Clone the repo with sparse-checkout if it doesn't already exist.
    Returns the absolute path to the repo.
    """
    repo_dir = Config.REPO_DIR
    data_subdir = Config.DATA_SUBDIR

    if os.path.isdir(os.path.join(repo_dir, ".git")):
        # Already cloned — make sure sparse-checkout pattern is set and pull
        _run(
            ["git", "sparse-checkout", "set", data_subdir],
            cwd=repo_dir,
        )
        _run(
            ["git", "pull", "--rebase", "origin", Config.GITHUB_BRANCH],
            cwd=repo_dir,
            check=False,  # tolerate if nothing to pull
        )
        return repo_dir

    # Fresh clone — sparse checkout
    auth_url = (
        f"https://{Config.GITHUB_TOKEN}@github.com/"
        f"{Config.GITHUB_USERNAME}/{_repo_name_from_url()}.git"
    )

    _run(["git", "clone", "--filter=blob:none", "--no-checkout", auth_url, repo_dir])
    _run(["git", "sparse-checkout", "init", "--cone"], cwd=repo_dir)
    _run(["git", "sparse-checkout", "set", data_subdir], cwd=repo_dir)
    _run(["git", "checkout", Config.GITHUB_BRANCH], cwd=repo_dir)

    # Configure committer identity (required on PythonAnywhere)
    _run(["git", "config", "user.name", Config.GITHUB_USERNAME], cwd=repo_dir)
    _run(["git", "config", "user.email", Config.GITHUB_EMAIL], cwd=repo_dir)

    return repo_dir


def _repo_name_from_url() -> str:
    """Extract 'owner/repo' or just 'repo' from the configured URL."""
    url = Config.GITHUB_REPO_URL
    # e.g. https://github.com/owner/repo.git
    parts = url.rstrip("/").rstrip(".git").split("/")
    # return 'repo'
    return parts[-1]


def _generate_filename(data: dict) -> str:
    """
    Build a unique, deterministic filename from the payload so that
    re-submitting the exact same annotation overwrites rather than
    duplicates.

    Pattern: <video_hash>_frame<N>_<timestamp_epoch_ms>.json
    """
    video_hash = hashlib.sha256(data["video_url"].encode()).hexdigest()[:12]
    frame = data["frame_index"]

    # Parse timestamp to epoch ms for a compact, sortable component
    ts = data["timestamp"]
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    epoch_ms = int(dt.timestamp() * 1000)

    return f"{video_hash}_frame{frame}_{epoch_ms}.json"


def save_and_push(data: dict) -> dict:
    """
    1. Ensure the repo exists (sparse checkout).
    2. Write the JSON payload to a file.
    3. git add / commit / push.

    Returns a dict with metadata about what happened.
    """
    repo_dir = ensure_repo()
    target_dir = os.path.join(repo_dir, Config.DATA_SUBDIR)
    Path(target_dir).mkdir(parents=True, exist_ok=True)

    filename = _generate_filename(data)
    filepath = os.path.join(target_dir, filename)

    # ---- Write file ----
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # ---- Git add ----
    _run(["git", "add", filepath], cwd=repo_dir)

    # ---- Check if there's actually something to commit ----
    status = _run(["git", "status", "--porcelain"], cwd=repo_dir)
    if not status.stdout.strip():
        return {
            "status": "no_change",
            "message": "File already exists with identical content",
            "filename": filename,
        }

    # ---- Commit ----
    now_utc = datetime.now(timezone.utc).isoformat()
    commit_msg = (
        f"annotation: {filename}\n\n"
        f"video_url: {data['video_url']}\n"
        f"frame_index: {data['frame_index']}\n"
        f"submitted_at: {now_utc}"
    )
    _run(["git", "commit", "-m", commit_msg], cwd=repo_dir)

    # ---- Push ----
    _run(
        ["git", "push", "origin", Config.GITHUB_BRANCH],
        cwd=repo_dir,
    )

    # ---- Collect commit SHA ----
    sha_result = _run(["git", "rev-parse", "HEAD"], cwd=repo_dir)
    commit_sha = sha_result.stdout.strip()

    return {
        "status": "pushed",
        "filename": filename,
        "commit_sha": commit_sha,
        "pushed_at": now_utc,
    }
