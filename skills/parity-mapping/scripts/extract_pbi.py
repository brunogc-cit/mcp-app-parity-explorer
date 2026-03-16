"""Extract measures, columns, and table sources from Power BI Tabular Editor models."""

import json
import os
import re
from typing import Dict, List, Any

import config

# Regex to parse _fn_GetDataFromDBX("catalog", "schema", "table")
_DBX_RE = re.compile(
    r'_fn_GetDataFromDBX\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
)


def _read_json(path: str) -> dict:
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def _discover_models(root: str) -> List[Dict[str, str]]:
    """Find all semantic models by locating database.json files (case-insensitive)."""
    models = []
    for dirpath, dirnames, filenames in os.walk(root):
        lower_files = [f.lower() for f in filenames]
        if "database.json" in lower_files:
            rel = os.path.relpath(dirpath, root)
            parts = rel.split(os.sep)
            domain = parts[0] if len(parts) >= 2 else ""
            model_name = parts[-1]
            models.append({
                "domain": domain,
                "model_name": model_name,
                "path": dirpath,
            })
    return models


def _extract_measures(model_path: str) -> List[Dict[str, Any]]:
    """Walk tables/@Measures/measures/*.json inside a model."""
    measures = []
    tables_dir = os.path.join(model_path, "tables")
    if not os.path.isdir(tables_dir):
        return measures

    measures_dir = os.path.join(tables_dir, "@Measures", "measures")
    if os.path.isdir(measures_dir):
        for fname in os.listdir(measures_dir):
            if not fname.endswith(".json"):
                continue
            data = _read_json(os.path.join(measures_dir, fname))
            expr = data.get("expression", "")
            if isinstance(expr, list):
                expr = " ".join(expr)
            measures.append({
                "name": data.get("name", fname.replace(".json", "")),
                "expression": expr,
                "description": data.get("description", ""),
                "displayFolder": data.get("displayFolder", ""),
            })
    return measures


def _extract_table_info(table_dir: str) -> Dict[str, Any]:
    """Extract columns and partition source from a single table directory."""
    table_name = os.path.basename(table_dir)
    if table_name == "@Measures":
        return {}

    info: Dict[str, Any] = {
        "table_name": table_name,
        "columns": [],
        "source_catalog": None,
        "source_schema": None,
        "source_table": None,
    }

    # Columns
    cols_dir = os.path.join(table_dir, "columns")
    if os.path.isdir(cols_dir):
        for fname in os.listdir(cols_dir):
            if not fname.endswith(".json"):
                continue
            data = _read_json(os.path.join(cols_dir, fname))
            info["columns"].append({
                "name": data.get("name", fname.replace(".json", "")),
                "sourceColumn": data.get("sourceColumn", ""),
                "dataType": data.get("dataType", ""),
                "description": data.get("description", ""),
            })

    # Partition -> source table
    parts_dir = os.path.join(table_dir, "partitions")
    if os.path.isdir(parts_dir):
        for fname in os.listdir(parts_dir):
            if not fname.endswith(".json"):
                continue
            data = _read_json(os.path.join(parts_dir, fname))
            source = data.get("source", {})
            expr = source.get("expression", "")
            if isinstance(expr, list):
                expr = " ".join(expr)
            m = _DBX_RE.search(expr)
            if m:
                info["source_catalog"] = m.group(1)
                info["source_schema"] = m.group(2)
                info["source_table"] = m.group(3)
                break

    return info


def extract_all_models() -> Dict[str, Any]:
    """
    Return a structured dict of all PBI semantic models:
    {
      "models": {
        "<model_name>": {
          "domain": str,
          "measures": [...],
          "tables": {
            "<table_name>": {
              "columns": [...],
              "source_catalog": str,
              "source_schema": str,
              "source_table": str,
            }
          }
        }
      }
    }
    """
    root = config.PBI_MODELS_DIR
    if not os.path.isdir(root):
        print(f"  WARNING: PBI models dir not found: {root}")
        return {"models": {}}

    discovered = _discover_models(root)
    result: Dict[str, Any] = {}

    for model_info in discovered:
        mname = model_info["model_name"]
        mpath = model_info["path"]

        measures = _extract_measures(mpath)
        tables: Dict[str, Any] = {}
        tables_dir = os.path.join(mpath, "tables")
        if os.path.isdir(tables_dir):
            for tname in os.listdir(tables_dir):
                tpath = os.path.join(tables_dir, tname)
                if not os.path.isdir(tpath) or tname == "@Measures":
                    continue
                tinfo = _extract_table_info(tpath)
                if tinfo:
                    tables[tname] = tinfo

        result[mname] = {
            "domain": model_info["domain"],
            "measures": measures,
            "tables": tables,
        }

    total_measures = sum(len(m["measures"]) for m in result.values())
    total_tables = sum(len(m["tables"]) for m in result.values())
    print(f"  PBI: {len(result)} models, {total_measures} measures, {total_tables} tables extracted")
    return {"models": result}


# ── Flat index builders for matching ────────────────────────────────────────

def build_measure_index(pbi_data: Dict) -> List[Dict[str, str]]:
    """Flat list of all measures with model context."""
    index = []
    for model_name, model in pbi_data.get("models", {}).items():
        for m in model.get("measures", []):
            index.append({
                "pbi_model": model_name,
                "pbi_name": m["name"],
                "pbi_type": "Measure",
                "expression": m.get("expression", ""),
                "displayFolder": m.get("displayFolder", ""),
                "description": m.get("description", ""),
            })
    return index


def build_column_index(pbi_data: Dict) -> List[Dict[str, str]]:
    """Flat list of all columns with table/model context."""
    index = []
    for model_name, model in pbi_data.get("models", {}).items():
        for table_name, table in model.get("tables", {}).items():
            src = ""
            if table.get("source_catalog") and table.get("source_table"):
                src = f"{table['source_catalog']}.{table['source_schema']}.{table['source_table']}"
            for col in table.get("columns", []):
                index.append({
                    "pbi_model": model_name,
                    "pbi_table": table_name,
                    "pbi_name": col["name"],
                    "pbi_type": "Column",
                    "sourceColumn": col.get("sourceColumn", ""),
                    "source_table_fqn": src,
                    "description": col.get("description", ""),
                })
    return index


if __name__ == "__main__":
    data = extract_all_models()
    for mname, mdata in data["models"].items():
        print(f"  {mname}: {len(mdata['measures'])} measures, {len(mdata['tables'])} tables")
