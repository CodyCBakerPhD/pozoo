import json
import os
import subprocess
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import logging
import traceback
from functools import wraps

from flask import Flask, request, jsonify


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config.from_object(Config)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Simple bearer-token auth decorator (optional but recommended)
# ---------------------------------------------------------------------------


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Missing Authorization header"}), 401
        token = auth.split(" ", 1)[1]
        if token != Config.API_SECRET:
            return jsonify({"error": "Invalid API key"}), 403
        return f(*args, **kwargs)

    return decorated


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/", methods=["GET"])
def health():
    """Simple health check."""
    return jsonify({"status": "ok", "service": "annotation-receiver"})


@app.route("/api/annotations", methods=["POST"])
@require_api_key
def receive_annotation():
    """
    Receive an annotation payload:
      1. Parse JSON body.
      2. Validate schema & semantics.
      3. Write to the Git repo, commit, push.
      4. Return result.
    """

    # ---- 1. Parse ----
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 415

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Could not parse JSON body"}), 400

    # ---- 2. Validate ----
    try:
        validated = validate_payload(data)
    except ValidationError as ve:
        logger.warning("Validation failed: %s", ve.errors)
        return (
            jsonify(
                {
                    "error": "Validation failed",
                    "details": ve.errors,
                }
            ),
            422,
        )

    # ---- 3. Save & Push ----
    try:
        result = save_and_push(validated)
    except Exception:
        tb = traceback.format_exc()
        logger.error("Git operation failed:\n%s", tb)
        return (
            jsonify(
                {
                    "error": "Failed to save annotation to repository",
                    "details": tb,
                }
            ),
            500,
        )

    # ---- 4. Respond ----
    logger.info("Annotation saved: %s", result)
    status_code = 200 if result["status"] == "no_change" else 201
    return jsonify(result), status_code


@app.route("/api/annotations", methods=["GET"])
@require_api_key
def list_annotations():
    """List files currently in the annotations directory."""
    import os
    from config import Config as C

    target = os.path.join(C.REPO_DIR, C.DATA_SUBDIR)
    if not os.path.isdir(target):
        return jsonify({"files": []})

    files = sorted(f for f in os.listdir(target) if f.endswith(".json"))
    return jsonify({"count": len(files), "files": files})


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


class Config:
    # GitHub settings
    GITHUB_REPO_URL = os.environ.get(
        "GITHUB_REPO_URL", "https://{token}@github.com/yourusername/yourrepo.git"
    )
    GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "ghp_xxxxxxxxxxxxxxxxxxxx")
    GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "yourusername")
    GITHUB_EMAIL = os.environ.get("GITHUB_EMAIL", "you@example.com")
    GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")

    # Paths
    # PythonAnywhere home directory
    HOME_DIR = os.path.expanduser("~")
    REPO_DIR = os.path.join(HOME_DIR, "label-data-repo")
    DATA_SUBDIR = "annotations"  # subdirectory inside the repo for JSON files

    # Validation
    REQUIRED_LABEL_IDS = {
        "left_front_paw",
        "right_front_paw",
        "left_hind_paw",
        "right_hind_paw",
        "nose",
        "tail_base",
    }

    # Auth token for incoming requests (optional but recommended)
    API_SECRET = os.environ.get("API_SECRET", "change-me-to-a-real-secret")


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


# ---------------------------------------------------------------------------
# Local dev server (PythonAnywhere uses WSGI, so this is only for local dev)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
