"""Extract column metadata from DBT serve layer YAML contracts."""

import os
import re
from typing import Dict, List, Any

import config

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


def _parse_yaml(path: str) -> dict:
    if yaml:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    # Minimal fallback parser for simple YAML contract files
    return _fallback_parse(path)


def _fallback_parse(path: str) -> dict:
    """Crude regex-based parser for serve contracts when PyYAML is unavailable."""
    with open(path, encoding="utf-8") as f:
        text = f.read()

    result: Dict[str, Any] = {"models": []}
    model_name_match = re.search(r"- name:\s*(\S+)", text)
    alias_match = re.search(r"alias:\s*(\S+)", text)

    if not model_name_match:
        return result

    model: Dict[str, Any] = {
        "name": model_name_match.group(1),
        "alias": alias_match.group(1) if alias_match else None,
        "columns": [],
    }

    for m in re.finditer(r"^\s{6}- name:\s*(\S+)\s*$", text, re.MULTILINE):
        col_name = m.group(1)
        model["columns"].append({"name": col_name})

    result["models"].append(model)
    return result


def _discover_contracts(root: str) -> List[str]:
    """Find all serve contract YAML files."""
    contracts = []
    for dirpath, _, filenames in os.walk(root):
        if "serve" not in dirpath:
            continue
        if "_contracts" not in os.path.basename(dirpath):
            continue
        for fname in filenames:
            if fname.startswith("serve_") and fname.endswith(".yml"):
                contracts.append(os.path.join(dirpath, fname))
    return contracts


def extract_serve_columns() -> Dict[str, List[Dict[str, str]]]:
    """
    Return a dict keyed by serve table alias (e.g. "fact_order_line_v1")
    mapping to lists of column dicts with name and optional description.
    """
    root = config.DBT_MODELS_DIR
    if not os.path.isdir(root):
        print(f"  WARNING: DBT models dir not found: {root}")
        return {}

    contracts = _discover_contracts(root)
    table_columns: Dict[str, List[Dict[str, str]]] = {}

    for cpath in contracts:
        try:
            data = _parse_yaml(cpath)
        except Exception as e:
            print(f"  WARNING: Failed to parse {cpath}: {e}")
            continue

        for model in data.get("models", []):
            name = model.get("name", "")
            alias = model.get("config", {}).get("alias") if isinstance(model.get("config"), dict) else model.get("alias")
            key = alias or name.replace("serve_", "", 1)

            cols = []
            for col in model.get("columns", []):
                cols.append({
                    "name": col.get("name", ""),
                    "data_type": col.get("data_type", ""),
                })
            if cols:
                table_columns[key] = cols

    total_cols = sum(len(v) for v in table_columns.values())
    print(f"  DBT: {len(table_columns)} serve tables, {total_cols} columns extracted")
    return table_columns


if __name__ == "__main__":
    cols = extract_serve_columns()
    for tbl, columns in sorted(cols.items())[:5]:
        print(f"  {tbl}: {len(columns)} columns")
        for c in columns[:3]:
            print(f"    - {c['name']}")
