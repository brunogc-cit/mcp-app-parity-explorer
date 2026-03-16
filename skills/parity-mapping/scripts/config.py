"""Configuration for the MSTR-to-PBI mapping tool.

All paths and credentials are configurable via environment variables
so the tool works both locally and inside Docker.

Repository discovery uses a 3-tier strategy (mirrors CLAUDE.md):
  1. Environment variable (PBI_MODELS_DIR / DBT_MODELS_DIR)
  2. config.json — reads repositories.powerbi / repositories.dbt
  3. Wildcard search — scans parent/sibling directories for matching patterns
"""

import glob
import json
import os

_TOOL_DIR = os.path.dirname(os.path.abspath(__file__))

# Neo4j (no hardcoded password — must be set via env var for live extraction)
NEO4J_URL = os.environ.get(
    "NEO4J_URL",
    "http://neo4jprod.uksouth.cloudapp.azure.com:7474/db/neo4j/tx/commit",
)
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

# Workspace root — used as base for repo discovery
WORKSPACE_ROOT = os.environ.get(
    "WORKSPACE_ROOT",
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_TOOL_DIR)))),
)


def _load_config_json() -> dict:
    """Load config.json from workspace root or common locations."""
    candidates = [
        os.path.join(WORKSPACE_ROOT, "config.json"),
        os.path.join(WORKSPACE_ROOT, "asos-agentic-workflow", "config.json"),
        os.path.join(WORKSPACE_ROOT, "asos-agentic-workflow", "web-app", "config.json"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            try:
                with open(p) as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
    return {}


def _find_repo(config_key: str, search_pattern: str, subpath: str) -> str:
    """3-tier repository discovery.

    Args:
        config_key: Key in config.json repositories (e.g. "powerbi")
        search_pattern: Glob pattern for wildcard search (e.g. "*data*ade*powerbi*")
        subpath: Path within the repo to the target directory (e.g. "powerbi/models")

    Returns:
        Absolute path to the target directory, or empty string if not found.
    """
    cfg = _load_config_json()
    repos = cfg.get("repositories", {})

    # Tier 1: config.json
    if config_key in repos:
        repo_path = repos[config_key]
        # Resolve relative paths against workspace root
        if not os.path.isabs(repo_path):
            repo_path = os.path.join(WORKSPACE_ROOT, repo_path)
        target = os.path.join(repo_path, subpath)
        if os.path.isdir(target):
            return target
        # config.json path exists but subpath doesn't — try repo root
        if os.path.isdir(repo_path):
            return repo_path

    # Tier 2: Search workspace root siblings and common locations
    search_dirs = [
        WORKSPACE_ROOT,
        os.path.dirname(WORKSPACE_ROOT),  # parent of workspace root
    ]
    # Also check repos/ subdirectory (Docker pattern)
    repos_dir = os.path.join(WORKSPACE_ROOT, "repos")
    if os.path.isdir(repos_dir):
        search_dirs.insert(0, repos_dir)

    for base in search_dirs:
        matches = sorted(glob.glob(os.path.join(base, search_pattern)))
        for match in matches:
            if os.path.isdir(match):
                target = os.path.join(match, subpath)
                if os.path.isdir(target):
                    return target
                # Repo found but subpath missing — return repo root
                return match

    return ""


def _find_cache() -> str:
    """Find mstr_cache.json — check skill scripts dir, then tools dir."""
    candidates = [
        os.path.join(_TOOL_DIR, "mstr_cache.json"),
        os.path.join(WORKSPACE_ROOT, "tools", "mstr-pbi-mapping", "mstr_cache.json"),
        os.path.join(os.path.dirname(WORKSPACE_ROOT), "tools", "mstr-pbi-mapping", "mstr_cache.json"),
    ]
    # Also check /app/tools/ (Docker)
    candidates.append("/app/tools/mstr-pbi-mapping/mstr_cache.json")
    for p in candidates:
        if os.path.isfile(p):
            return p
    return os.path.join(_TOOL_DIR, "mstr_cache.json")  # default even if missing


# Repository paths — 3-tier discovery
PBI_MODELS_DIR = os.environ.get("PBI_MODELS_DIR") or _find_repo(
    config_key="powerbi",
    search_pattern="*data*ade*powerbi*",
    subpath=os.path.join("powerbi", "models"),
)
DBT_MODELS_DIR = os.environ.get("DBT_MODELS_DIR") or _find_repo(
    config_key="dbt",
    search_pattern="*data*ade*dbt*",
    subpath=os.path.join("bundles", "core_data", "models"),
)

OUTPUT_DIR = os.environ.get(
    "OUTPUT_DIR",
    os.path.join(_TOOL_DIR, "output"),
)

# Cache path — multi-location discovery
CACHE_PATH = os.environ.get("MSTR_CACHE_PATH") or _find_cache()

# Signal weights (when S1 is absent)
WEIGHT_S2_LINEAGE = 0.30
WEIGHT_S3_NAME = 0.35
WEIGHT_S4_FORMULA = 0.25
WEIGHT_S5_CONTEXT = 0.10

# Confidence thresholds
THRESHOLD_CONFIRMED = 0.90
THRESHOLD_HIGH = 0.70
THRESHOLD_MEDIUM = 0.50
THRESHOLD_LOW = 0.30
