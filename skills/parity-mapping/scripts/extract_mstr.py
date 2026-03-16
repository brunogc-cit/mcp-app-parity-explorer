"""Extract all Metrics and Attributes from the MicroStrategy Neo4j graph.

Supports two modes:
  1. Live: queries Neo4j HTTP API directly (requires network access)
  2. Cache: reads from a local mstr_cache.json file (default fallback)

The cache file can be regenerated via the MCP tool in Cursor or by running
this module directly with --refresh (when Neo4j is reachable).
"""

import json
import os
import requests
from typing import Dict, List, Any

import config

_CACHE_PATH = config.CACHE_PATH


def _execute_cypher(query: str, parameters: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Execute a read-only Cypher query and return rows as dicts."""
    payload = {
        "statements": [
            {"statement": query, "parameters": parameters or {}}
        ]
    }
    resp = requests.post(
        config.NEO4J_URL,
        json=payload,
        auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        raise RuntimeError(f"Neo4j errors: {body['errors']}")

    rows: list[dict] = []
    for stmt in body.get("results", []):
        cols = stmt.get("columns", [])
        for row_data in stmt.get("data", []):
            rows.append(dict(zip(cols, row_data.get("row", []))))
    return rows


# ── Bulk extraction queries ────────────────────────────────────────────────

_METRICS_QUERY = """
MATCH (m:Metric)
RETURN m.guid                      AS guid,
       m.name                      AS name,
       'Metric'                    AS type,
       m.formula                   AS formula,
       m.parity_status             AS parity_status,
       m.updated_parity_status     AS updated_parity_status,
       m.edw_table                 AS edw_table,
       m.edw_column                AS edw_column,
       m.ade_db_table              AS ade_table,
       m.ade_db_column             AS ade_column,
       m.db_raw                    AS db_raw,
       m.db_serve                  AS db_serve,
       m.pb_semantic               AS pb_semantic,
       m.pb_semantic_model         AS pb_semantic_model,
       m.pb_semantic_name          AS pb_semantic_name,
       m.updated_pb_semantic_model AS updated_pb_semantic_model,
       m.updated_pb_semantic_name  AS updated_pb_semantic_name,
       m.updated_edw_table         AS updated_edw_table,
       m.updated_edw_column        AS updated_edw_column,
       m.updated_ade_db_table      AS updated_ade_db_table,
       m.inherited_priority_level  AS priority,
       m.lineage_source_tables     AS lineage_source_tables
SKIP $skip LIMIT $limit
"""

_ATTRIBUTES_QUERY = """
MATCH (a:Attribute)
RETURN a.guid                      AS guid,
       a.name                      AS name,
       'Attribute'                 AS type,
       null                        AS formula,
       a.parity_status             AS parity_status,
       a.updated_parity_status     AS updated_parity_status,
       a.edw_table                 AS edw_table,
       a.edw_column                AS edw_column,
       a.ade_db_table              AS ade_table,
       a.ade_db_column             AS ade_column,
       a.db_raw                    AS db_raw,
       a.db_serve                  AS db_serve,
       a.pb_semantic               AS pb_semantic,
       a.pb_semantic_model         AS pb_semantic_model,
       a.pb_semantic_name          AS pb_semantic_name,
       a.updated_pb_semantic_model AS updated_pb_semantic_model,
       a.updated_pb_semantic_name  AS updated_pb_semantic_name,
       a.updated_edw_table         AS updated_edw_table,
       a.updated_edw_column        AS updated_edw_column,
       a.updated_ade_db_table      AS updated_ade_db_table,
       a.inherited_priority_level  AS priority,
       a.lineage_source_tables     AS lineage_source_tables,
       a.forms_json                AS forms_json
SKIP $skip LIMIT $limit
"""

PAGE_SIZE = 2000


def _paginate(query: str) -> List[Dict[str, Any]]:
    all_rows: list[dict] = []
    skip = 0
    while True:
        rows = _execute_cypher(query, {"skip": skip, "limit": PAGE_SIZE})
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return all_rows


def extract_metrics() -> List[Dict[str, Any]]:
    """Return all MSTR Metrics with their properties."""
    return _paginate(_METRICS_QUERY)


def extract_attributes() -> List[Dict[str, Any]]:
    """Return all MSTR Attributes with their properties."""
    return _paginate(_ATTRIBUTES_QUERY)


def _extract_live() -> List[Dict[str, Any]]:
    """Fetch from Neo4j directly (requires network access)."""
    metrics = extract_metrics()
    attrs = extract_attributes()
    print(f"  Neo4j (live): {len(metrics)} metrics, {len(attrs)} attributes extracted")
    return metrics + attrs


def _extract_from_cache() -> List[Dict[str, Any]]:
    """Read from local mstr_cache.json (prioritized objects only)."""
    with open(_CACHE_PATH, encoding="utf-8") as f:
        items = json.load(f)
    metrics = [i for i in items if i.get("type") == "Metric"]
    attrs = [i for i in items if i.get("type") == "Attribute"]
    prioritized = [i for i in items if i.get("priority") is not None]
    print(f"  Neo4j (cache): {len(metrics)} metrics, {len(attrs)} attributes loaded")
    if prioritized:
        print(f"  Scope: prioritized objects only ({len(prioritized)} with priority level)")
    return items


def extract_all(use_cache: bool = True) -> List[Dict[str, Any]]:
    """
    Return combined list of all Metrics and Attributes.
    Tries cache first (if available and use_cache=True), falls back to live.
    """
    if use_cache and os.path.isfile(_CACHE_PATH):
        return _extract_from_cache()

    try:
        items = _extract_live()
        # Save cache for next time
        with open(_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(items, f)
        print(f"  Cache saved to {_CACHE_PATH}")
        return items
    except Exception as e:
        if os.path.isfile(_CACHE_PATH):
            print(f"  WARNING: Neo4j unreachable ({e}), using cache")
            return _extract_from_cache()
        raise


if __name__ == "__main__":
    import sys
    refresh = "--refresh" in sys.argv
    items = extract_all(use_cache=not refresh)
    print(f"Total MSTR objects: {len(items)}")
    with_mapping = [i for i in items if i.get("pb_semantic_name") or i.get("updated_pb_semantic_name")]
    print(f"With PBI mapping: {len(with_mapping)}")
