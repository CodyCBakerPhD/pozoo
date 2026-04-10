# Consolidated into a single file for easier linting
import dataclasses
import json
import os
import pathlib
import subprocess
import time
import traceback
import typing
import uuid
from pathlib import Path

import flask
import flask_restx
import packaging.version
import typing_extensions

app = flask.Flask(__name__)
api = flask_restx.Api(
    version="0.2",
    title="upload-nwb-benchmarks-results",
    description="Automatic uploader of NWB Benchmark results.",
)
api.init_app(app)

data_namespace = flask_restx.Namespace(name="data", description="API route for data.")
api.add_namespace(data_namespace)

contribute_parser = flask_restx.reqparse.RequestParser()
contribute_parser.add_argument(
    "filename", type=str, required=True, help="Name of the file to upload."
)
contribute_parser.add_argument(
    "test", type=bool, required=False, default=False, help="Test mode flag."
)


@data_namespace.route("/contribute")
class Contribute(flask_restx.Resource):
    @data_namespace.expect(contribute_parser)
    @data_namespace.doc(
        responses={
            200: "Success",
            400: "Bad Request",
            500: "Internal server error",
        }
    )
    def post(self) -> int:
        try:
            arguments = contribute_parser.parse_args()
            filename = pathlib.Path(arguments["filename"])
            test_mode = arguments["test"]

            payload = data_namespace.payload
            json_content = payload["json_content"]

            manager = GitHubResultsManager(repo_name="nwb-benchmarks-results")
            result = manager.ensure_repo_up_to_date()
            if result is not None:
                return result

            time.sleep(1)

            manager.write_file(filename=filename, json_content=json_content)

            time.sleep(1)

            if test_mode is False:
                result = manager.add_and_commit(message="Add new benchmark results")
                if result is not None:
                    return result

                time.sleep(1)

                manager.push()

            return 200
        except Exception as exception:
            return {
                "type": type(exception).__name__,
                "error": str(exception),
                "traceback": traceback.format_exc(),
            }, 500


@data_namespace.route("/update-database")
class UpdateDataBase(flask_restx.Resource):
    @data_namespace.doc(
        responses={
            200: "Success",
            400: "Bad Request",
            500: "Internal server error",
            521: "Repository path on server not found.",
            522: "Error during local update. Check server logs for specifics.",
            523: "Error during add/commit. Check server logs for specifics.",
        }
    )
    def post(self) -> int:
        try:
            directory = (
                Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-results"
            )
            output_directory = (
                Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-database"
            )

            manager = GitHubResultsManager(repo_name="nwb-benchmarks-database")
            result = manager.ensure_repo_up_to_date()
            if result is not None:
                return result

            time.sleep(1)

            repackage_as_parquet(directory=directory, output_directory=output_directory)

            time.sleep(1)

            result = manager.add_and_commit(message="Update benchmark databases")
            if result is not None:
                return result

            time.sleep(1)

            manager.push()

            return 200
        except Exception as exception:
            return {
                "type": type(exception).__name__,
                "error": str(exception),
                "traceback": traceback.format_exc(),
            }, 500


class GitHubResultsManager:
    def __init__(self, repo_name: str):
        self.cache_directory = Path.home() / ".cache" / "nwb-benchmarks"
        self.cache_directory.mkdir(parents=True, exist_ok=True)
        self.repo_path = self.cache_directory / repo_name

    def ensure_repo_up_to_date(self) -> typing.Literal[521, 522] | None:
        """Clone repository if it doesn't exist locally."""
        if not self.repo_path.exists():
            return 521

        command = "git pull"
        cwd = self.repo_path
        result = subprocess.run(
            args=command,
            cwd=cwd,
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}"
            print(f"ERROR: {message}")
            return 522

    def write_file(self, filename: pathlib.Path, json_content: dict) -> None:
        """Write results JSON to a file in the cache directory."""
        base_directory = self.cache_directory / "nwb-benchmarks-results"
        filestem: str = filename.stem
        if filestem.startswith("environment-"):
            directory = base_directory / "environments"
        elif filestem.startswith("machine-"):
            directory = base_directory / "machines"
        elif filestem.endswith("_results"):
            directory = base_directory / "results"
        else:
            # Legacy outer collection
            directory = base_directory
        file_path = directory / filename

        with open(file=file_path, mode="w") as file_stream:
            json.dump(obj=json_content, fp=file_stream, indent=4)

    def add_and_commit(self, message: str) -> typing.Literal[523] | None:
        """Commit results to git repo."""
        command = f"git add . && git commit -m '{message}'"
        result = subprocess.run(
            args=command,
            cwd=self.repo_path,
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}\ntraceback: {traceback.format_exc()}"
            print(f"ERROR: {message}")
            return 523

    def push(self):
        """Commit and push results to GitHub repository."""
        command = "git push"
        cwd = self.repo_path
        result = subprocess.run(
            args=command,
            cwd=cwd,
            capture_output=True,
            shell=True,
        )
        if result.returncode != 0:
            message = f"Git command ({command}) failed: {result.stderr.decode()}"
            raise RuntimeError(message)


@dataclasses.dataclass
class Machine:
    id: str
    name: str
    version: str
    os: dict
    sys: dict
    platform: dict
    psutil: dict
    cuda: dict
    asv: dict

    @classmethod
    def safe_load_from_json(
        cls, file_path: pathlib.Path
    ) -> typing_extensions.Self | None:
        with file_path.open(mode="r") as file_stream:
            data = json.load(file_stream)

        version = str(data.get("version", None))
        if version is None or packaging.version.Version(
            version
        ) < packaging.version.Version(version="1.1.0"):
            return None

        machine_id = file_path.stem.removeprefix("machine-")

        return cls(
            id=machine_id,
            name=data.get("name", ""),
            version=version,
            os=data.get("os", {}),
            sys=data.get("sys", {}),
            platform=data.get("platform", {}),
            psutil=data.get("psutil", {}),
            cuda=data.get("cuda", {}),
            asv=data.get("asv", {}),
        )

    def to_dataframe(self) -> "polars.DataFrame":
        import polars

        data = {
            "name": self.name,
            "version": self.version,
            "os": json.dumps(self.os),
            "sys": json.dumps(self.sys),
            "platform": json.dumps(self.platform),
            "psutil": json.dumps(self.psutil),
            "cuda": json.dumps(self.cuda),
            "asv": json.dumps(self.asv),
        }

        data_frame = polars.DataFrame(data=data)
        return data_frame


@dataclasses.dataclass
class Environment:
    environment_id: str
    preamble: str

    # Allow arbitrary fields
    def __init__(self, environment_id: str, preamble: str, **kwargs) -> None:
        self.environment_id = environment_id
        self.preamble = preamble
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def safe_load_from_json(
        cls, file_path: pathlib.Path
    ) -> typing_extensions.Self | None:
        with file_path.open(mode="r") as file_stream:
            data = json.load(fp=file_stream)

        if len(data) > 1:
            return None

        environment_id = file_path.stem.removeprefix("environment-")
        preamble = next(iter(data.keys()))

        packages = {
            package["name"]: f"{package["version"]} ({package["build"]})"
            for package in data[preamble]
            if len(package) == 3
        }

        if not any(packages):
            return None

        return cls(environment_id=environment_id, preamble=preamble, **packages)

    def to_dataframe(self) -> "polars.DataFrame":
        import polars

        data = {
            "environment_id": self.environment_id,
            "preamble": self.preamble,
        }
        for package_name, package_details in self.__dict__.items():
            if package_name not in ["environment_id", "preamble"]:
                data[package_name] = package_details

        data_frame = polars.DataFrame(data=data, orient="col")
        return data_frame


@dataclasses.dataclass
class Result:
    uuid: str
    version: str
    timestamp: str
    commit_hash: str
    environment_id: str
    machine_id: str
    benchmark_name: str
    parameter_case: str
    value: float


@dataclasses.dataclass
class Results:
    results: list[Result]

    @classmethod
    def safe_load_from_json(
        cls, file_path: pathlib.Path
    ) -> typing_extensions.Self | None:
        with file_path.open(mode="r") as file_stream:
            data = json.load(fp=file_stream)

        database_version = data.get("database_version", None)
        if database_version is None or packaging.version.Version(
            data["database_version"]
        ) < packaging.version.Version(version="1.0.0"):
            return None

        timestamp = data["timestamp"]
        commit_hash = data["commit_hash"]
        environment_id = data["environment_id"]
        machine_id = data["machine_id"]

        results = [
            Result(
                uuid=str(
                    uuid.uuid4()
                ),  # TODO: add this to each results file so it is persistent
                version=database_version,
                timestamp=timestamp,
                commit_hash=commit_hash,
                environment_id=environment_id,
                machine_id=machine_id,
                benchmark_name=benchmark_name,
                parameter_case=parameter_case,
                value=benchmark_result,
            )
            for benchmark_name, parameter_cases in data["results"].items()
            for parameter_case, benchmark_results in parameter_cases.items()
            for benchmark_result in benchmark_results
        ]
        return cls(results=results)

    def to_dataframe(self) -> "polars.DataFrame":
        import polars

        data = {
            "uuid": [result.uuid for result in self.results],
            "version": [result.version for result in self.results],
            "commit_hash": [result.commit_hash for result in self.results],
            "environment_id": [result.environment_id for result in self.results],
            "machine_id": [result.machine_id for result in self.results],
            "benchmark_name": [result.benchmark_name for result in self.results],
            "parameter_case": [result.parameter_case for result in self.results],
            "value": [result.value for result in self.results],
        }

        data_frame = polars.DataFrame(data=data)
        return data_frame


def repackage_as_parquet(
    directory: pathlib.Path, output_directory: pathlib.Path
) -> None:
    import polars

    # Machines
    machines_data_frames = []
    machines_directory = directory / "machines"
    for machine_file_path in machines_directory.iterdir():
        machine = Machine.safe_load_from_json(file_path=machine_file_path)

        if machine is None:
            continue

        machine_data_frame = machine.to_dataframe()
        machines_data_frames.append(machine_data_frame)
    machines_database = polars.concat(
        items=machines_data_frames, how="diagonal_relaxed"
    )

    machines_database_file_path = output_directory / "machines.parquet"
    machines_database.write_parquet(file=machines_database_file_path)

    # Environments
    environments_data_frames = []
    environments_directory = directory / "environments"
    for environment_file_path in environments_directory.iterdir():
        environment = Environment.safe_load_from_json(file_path=environment_file_path)

        if environment is None:
            continue

        environment_data_frame = environment.to_dataframe()
        environments_data_frames.append(environment_data_frame)
    environments_database = polars.concat(
        items=environments_data_frames, how="diagonal"
    )

    environments_database_file_path = output_directory / "environments.parquet"
    environments_database.write_parquet(file=environments_database_file_path)

    # Results
    all_results_data_frames = []
    results_directory = directory / "results"
    for result_file_path in results_directory.iterdir():
        results = Results.safe_load_from_json(file_path=result_file_path)

        if results is None:
            continue

        results_data_frame = results.to_dataframe()
        all_results_data_frames.append(results_data_frame)
    all_results_database = polars.concat(items=all_results_data_frames, how="diagonal")

    all_results_database_file_path = output_directory / "results.parquet"
    all_results_database.write_parquet(file=all_results_database_file_path)


# ******
# Pose Zoo testing
# ******

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


"""
Manages the sparse-checkout Git repository and all write / commit / push
operations.
"""



"""
PythonAnywhere Flask app — receives annotation JSON, validates it,
writes it to a sparse-checkout GitHub repo, commits and pushes.
"""

import logging
import traceback
from functools import wraps

from flask import Flask, request, jsonify

from validators import validate_payload, ValidationError
from git_manager import save_and_push

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


# ---------------------------------------------------------------------------
# Local dev server (PythonAnywhere uses WSGI, so this is only for local dev)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)


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


if __name__ == "__main__":
    DEBUG_MODE = os.environ.get("NWB_BENCHMARKS_DEBUG", None)
    if DEBUG_MODE is not None and DEBUG_MODE != "1":
        message = "NWB_BENCHMARKS_DEBUG environment variable must be set to '1' to run the Flask app in debug mode."
        raise ValueError(message)

    if DEBUG_MODE == "1":
        app.run(debug=True, host="127.0.0.1", port=5000)
    else:
        app.run()
