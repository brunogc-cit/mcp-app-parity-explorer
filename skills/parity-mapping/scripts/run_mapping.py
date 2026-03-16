#!/usr/bin/env python3
"""
MSTR → Power BI Migration Mapping Tool

Extracts all MicroStrategy metrics/attributes, Power BI semantic model
definitions, and DBT serve layer metadata, then applies 5 matching signals
to produce a Markdown report with confidence levels and migration coverage.

Usage:
    python run_mapping.py [--output DIR]
    python run_mapping.py --filter "Retail Sales Value,Sell Through %" --output DIR
    python run_mapping.py --filter-file scope.json --scope-label "Heart Dashboard" --output DIR
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import config
import extract_mstr
import extract_pbi
import extract_dbt
import signals


# ═══════════════════════════════════════════════════════════════════════════
# Scoring engine
# ═══════════════════════════════════════════════════════════════════════════

def _classify(score: float) -> str:
    if score >= config.THRESHOLD_CONFIRMED:
        return "Confirmed"
    if score >= config.THRESHOLD_HIGH:
        return "High"
    if score >= config.THRESHOLD_MEDIUM:
        return "Medium"
    if score >= config.THRESHOLD_LOW:
        return "Low"
    return "Unmapped"


def _best_candidate(candidates: List[Dict]) -> Optional[Dict]:
    if not candidates:
        return None
    return max(candidates, key=lambda c: c.get("confidence", 0))


def score_object(
    mstr_obj: Dict,
    pbi_measures: List[Dict],
    pbi_columns: List[Dict],
    pbi_table_sources: Dict[str, str],
) -> Dict[str, Any]:
    """
    Apply all 5 signals to a single MSTR object and return the best mapping
    with a combined confidence score.
    """
    result: Dict[str, Any] = {
        "mstr_guid": mstr_obj.get("guid", ""),
        "mstr_name": mstr_obj.get("name", ""),
        "mstr_type": mstr_obj.get("type", ""),
        "mstr_formula": mstr_obj.get("formula", ""),
        "parity_status": mstr_obj.get("status") or mstr_obj.get("parity_status") or mstr_obj.get("updated_parity_status") or "",
        "priority": mstr_obj.get("priority", ""),
        "team": mstr_obj.get("team", ""),
        "report_count": mstr_obj.get("report_count", 0),
        "edw_table": mstr_obj.get("edw_table", ""),
        "edw_column": mstr_obj.get("edw_column", ""),
        "ade_table": mstr_obj.get("ade_table", ""),
        "ade_column": mstr_obj.get("ade_column", ""),
    }

    # S1: direct mapping — if present, it's authoritative
    s1 = signals.signal_s1(mstr_obj)
    if s1:
        result.update({
            "pbi_name": s1["pbi_name"],
            "pbi_model": s1["pbi_model"],
            "confidence": 1.0,
            "confidence_level": "Confirmed",
            "signals_used": "S1",
        })
        return result

    # Choose the right PBI target list by type
    is_metric = mstr_obj.get("type") == "Metric"
    pbi_targets = pbi_measures if is_metric else pbi_columns

    # S2: column lineage
    s2 = signals.signal_s2(mstr_obj, pbi_columns)

    # S3: name similarity
    s3_list = signals.signal_s3(mstr_obj, pbi_targets, top_k=3)
    s3 = _best_candidate(s3_list)

    # S4: formula analysis (metrics only)
    s4 = None
    if is_metric:
        s4_list = signals.signal_s4(mstr_obj, pbi_measures, top_k=3)
        s4 = _best_candidate(s4_list)

    # S5: table context (provides a bonus, not a standalone match)
    s5_score = signals.signal_s5(mstr_obj, pbi_table_sources)

    # Combine signals with weighted scoring
    # Find the best candidate across S2/S3/S4 and add S5 bonus
    candidates: List[Dict] = []
    if s2:
        candidates.append(s2)
    if s3:
        candidates.append(s3)
    if s4:
        candidates.append(s4)

    if not candidates:
        result.update({
            "pbi_name": "",
            "pbi_model": "",
            "confidence": 0.0,
            "confidence_level": "Unmapped",
            "signals_used": "",
        })
        return result

    # For each candidate PBI name, aggregate scores across signals
    by_pbi_name: Dict[str, Dict] = {}
    for c in candidates:
        key = c["pbi_name"]
        if key not in by_pbi_name:
            by_pbi_name[key] = {
                "pbi_name": c["pbi_name"],
                "pbi_model": c.get("pbi_model", ""),
                "s2": 0.0,
                "s3": 0.0,
                "s4": 0.0,
                "signals": [],
            }
        sig = c["signal"]
        by_pbi_name[key][sig.lower()] = c["confidence"]
        by_pbi_name[key]["signals"].append(sig)
        if c.get("pbi_model"):
            by_pbi_name[key]["pbi_model"] = c["pbi_model"]

    best_score = 0.0
    best_entry = None
    for entry in by_pbi_name.values():
        # Weighted average of all present signals
        weighted = (
            config.WEIGHT_S2_LINEAGE * entry["s2"]
            + config.WEIGHT_S3_NAME * entry["s3"]
            + config.WEIGHT_S4_FORMULA * entry["s4"]
            + config.WEIGHT_S5_CONTEXT * s5_score
        )
        # Also consider the best individual signal score (a perfect S3 match
        # alone should be enough for high confidence)
        individual_scores = [v for v in [entry["s2"], entry["s3"], entry["s4"]] if v > 0]
        best_individual = max(individual_scores) if individual_scores else 0.0

        # Multi-signal bonus: when multiple signals agree, boost confidence
        active_count = sum(1 for v in [entry["s2"], entry["s3"], entry["s4"]] if v > 0.3)
        multi_bonus = 0.10 * (active_count - 1) if active_count > 1 else 0.0

        # Final score: blend of weighted and best individual + multi bonus
        score = max(weighted, 0.6 * best_individual) + multi_bonus
        score = min(score, 0.99)  # cap below S1's 1.0

        if score > best_score:
            best_score = score
            best_entry = entry

    if best_entry and best_score >= config.THRESHOLD_LOW:
        used = "+".join(sorted(set(best_entry["signals"])))
        if s5_score > 0:
            used += "+S5"
        result.update({
            "pbi_name": best_entry["pbi_name"],
            "pbi_model": best_entry["pbi_model"],
            "confidence": round(best_score, 4),
            "confidence_level": _classify(best_score),
            "signals_used": used,
        })
    else:
        # Below threshold — report best guess anyway
        if best_entry:
            result.update({
                "pbi_name": best_entry["pbi_name"],
                "pbi_model": best_entry["pbi_model"],
                "confidence": round(best_score, 4),
                "confidence_level": "Unmapped",
                "signals_used": "+".join(sorted(set(best_entry["signals"]))),
            })
        else:
            result.update({
                "pbi_name": "",
                "pbi_model": "",
                "confidence": 0.0,
                "confidence_level": "Unmapped",
                "signals_used": "",
            })

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Scope filtering
# ═══════════════════════════════════════════════════════════════════════════

def _load_filter_names(filter_csv: Optional[str], filter_file: Optional[str]) -> Optional[List[str]]:
    """Resolve filter names from --filter (CSV string) or --filter-file (JSON)."""
    names: List[str] = []

    if filter_csv:
        names.extend(n.strip() for n in filter_csv.split(",") if n.strip())

    if filter_file:
        with open(filter_file, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            names.extend(str(n).strip() for n in data if str(n).strip())
        elif isinstance(data, dict):
            for key in ("metrics", "attributes", "filters", "kpis", "items"):
                if key in data and isinstance(data[key], list):
                    names.extend(str(n).strip() for n in data[key] if str(n).strip())
            if "label" in data and not names:
                pass  # label-only file, no names to extract

    return names if names else None


_TOKEN_SYNONYMS = {
    "thru": "through",
    "qty": "quantity",
    "units": "quantity",
    "pv": "views",
    "eod": "end",
    "eow": "end",
    "ltd": "date",
    "asp": "price",
    "fc": "warehouse",
}


def _tokenize(name: str) -> set:
    """Split a name into lowercase word tokens with domain-aware normalisation."""
    import re
    tokens = set(re.split(r'[\s/\-_()%]+', name.lower()))
    tokens.discard("")
    normalised = set()
    for t in tokens:
        normalised.add(_TOKEN_SYNONYMS.get(t, t))
    return normalised


def _token_similarity(a: set, b: set) -> float:
    """Jaccard similarity between two token sets."""
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _apply_filter(
    objects: List[Dict[str, Any]],
    filter_names: List[str],
) -> List[Dict[str, Any]]:
    """Filter MSTR objects to those matching the provided names.

    Matching strategy (progressively looser):
      1. Exact match (case-insensitive)
      2. Substring match (either direction)
      3. Token-based fuzzy match (Jaccard >= 0.5) — catches cases like
         "Total Sales Value" matching "Retail Sales Value" or
         "Product Views" matching "Product Page Views"
    Prints warnings for filter names with no match.
    """
    norm_names = [n.lower().strip() for n in filter_names]
    matched: List[Dict[str, Any]] = []
    matched_filter_indices: set = set()

    # Pass 1: exact match
    for obj in objects:
        obj_name = (obj.get("name") or "").lower().strip()
        for idx, fn in enumerate(norm_names):
            if obj_name == fn:
                matched.append(obj)
                matched_filter_indices.add(idx)
                break

    matched_guids = {obj.get("guid") for obj in matched}

    # Pass 2: substring match for remaining filter names
    unmatched_indices = [i for i in range(len(norm_names)) if i not in matched_filter_indices]
    for idx in unmatched_indices:
        fn = norm_names[idx]
        for obj in objects:
            if obj.get("guid") in matched_guids:
                continue
            obj_name = (obj.get("name") or "").lower().strip()
            if fn in obj_name or obj_name in fn:
                matched.append(obj)
                matched_guids.add(obj.get("guid"))
                matched_filter_indices.add(idx)
                break

    # Pass 3: token-based fuzzy match for still-unmatched filter names
    unmatched_indices = [i for i in range(len(norm_names)) if i not in matched_filter_indices]
    if unmatched_indices:
        filter_tokens = {idx: _tokenize(norm_names[idx]) for idx in unmatched_indices}
        for idx in unmatched_indices:
            best_score = 0.0
            best_obj = None
            ft = filter_tokens[idx]
            for obj in objects:
                if obj.get("guid") in matched_guids:
                    continue
                obj_name = (obj.get("name") or "").lower().strip()
                ot = _tokenize(obj_name)
                score = _token_similarity(ft, ot)
                if score > best_score:
                    best_score = score
                    best_obj = obj
            if best_obj and best_score >= 0.5:
                matched.append(best_obj)
                matched_guids.add(best_obj.get("guid"))
                matched_filter_indices.add(idx)

    # Report unmatched filter names
    still_unmatched = [filter_names[i] for i in range(len(filter_names)) if i not in matched_filter_indices]
    if still_unmatched:
        print(f"  NOTE: {len(still_unmatched)} filter name(s) had no match in the cache (may be new/calculated measures):")
        for name in still_unmatched:
            print(f"    - {name}")

    return matched


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def run_pipeline(
    filter_names: Optional[List[str]] = None,
    scope_label: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute the extraction + matching pipeline, optionally filtered to a subset."""
    print("=" * 60)
    print("MSTR → PBI Migration Mapping Tool")
    if scope_label:
        print(f"  Scope: {scope_label}")
    print("=" * 60)

    # Phase 1: Extract
    print("\n[Phase 1] Extracting data sources...")
    mstr_objects = extract_mstr.extract_all()

    if filter_names:
        total_before = len(mstr_objects)
        mstr_objects = _apply_filter(mstr_objects, filter_names)
        print(f"  Scope: filtered to {len(mstr_objects)}/{total_before} objects"
              f" (from {scope_label or 'user filter'})")

    pbi_data = extract_pbi.extract_all_models()
    pbi_measures = extract_pbi.build_measure_index(pbi_data)
    pbi_columns = extract_pbi.build_column_index(pbi_data)

    dbt_columns = extract_dbt.extract_serve_columns()

    # Build PBI table source index for S5
    pbi_table_sources: Dict[str, str] = {}
    for model_name, model in pbi_data.get("models", {}).items():
        for table_name, table in model.get("tables", {}).items():
            if table.get("source_table"):
                key = f"{model_name}/{table_name}"
                fqn = f"{table.get('source_catalog','')}.{table.get('source_schema','')}.{table['source_table']}"
                pbi_table_sources[key] = fqn

    print(f"\n  PBI targets: {len(pbi_measures)} measures, {len(pbi_columns)} columns")
    print(f"  DBT serve: {len(dbt_columns)} tables")
    print(f"  PBI table sources: {len(pbi_table_sources)} entries")

    # Phase 2: Score each MSTR object
    print(f"\n[Phase 2] Scoring {len(mstr_objects)} MSTR objects...")
    results = []
    for i, obj in enumerate(mstr_objects):
        if (i + 1) % 500 == 0:
            print(f"  ... scored {i + 1}/{len(mstr_objects)}")
        r = score_object(obj, pbi_measures, pbi_columns, pbi_table_sources)
        results.append(r)

    print(f"  Scoring complete: {len(results)} objects processed")
    return results


# ═══════════════════════════════════════════════════════════════════════════
# Markdown Report Generator
# ═══════════════════════════════════════════════════════════════════════════

def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    """Generate a Markdown table string."""
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        escaped = [str(c).replace("|", "\\|") for c in row]
        lines.append("| " + " | ".join(escaped) + " |")
    return "\n".join(lines)


def generate_report(
    results: List[Dict[str, Any]],
    output_dir: str,
    scope_label: Optional[str] = None,
) -> str:
    """Generate a Markdown report and write it to output_dir."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    metrics = [r for r in results if r["mstr_type"] == "Metric"]
    attrs = [r for r in results if r["mstr_type"] == "Attribute"]

    dropped = [r for r in results if r.get("parity_status") == "Drop"]
    dropped_m = [r for r in dropped if r["mstr_type"] == "Metric"]
    dropped_a = [r for r in dropped if r["mstr_type"] == "Attribute"]

    # "Mapped" excludes dropped items — they won't be migrated regardless
    mapped_metrics = [r for r in metrics if r["confidence_level"] != "Unmapped" and r.get("parity_status") != "Drop"]
    mapped_attrs = [r for r in attrs if r["confidence_level"] != "Unmapped" and r.get("parity_status") != "Drop"]

    # "In scope" = total minus dropped
    in_scope_m = len(metrics) - len(dropped_m)
    in_scope_a = len(attrs) - len(dropped_a)
    in_scope = in_scope_m + in_scope_a

    total = len(results)
    total_mapped = len(mapped_metrics) + len(mapped_attrs)

    unmapped_m_count = in_scope_m - len(mapped_metrics)
    unmapped_a_count = in_scope_a - len(mapped_attrs)

    def _pct(n: int, d: int) -> str:
        return f"{n / d * 100:.1f}%" if d else "0.0%"

    # Confidence distribution (only in-scope mapped)
    levels = ["Confirmed", "High", "Medium", "Low"]
    dist: Dict[str, Dict[str, int]] = {lv: {"metrics": 0, "attrs": 0} for lv in levels}
    for r in mapped_metrics:
        lv = r["confidence_level"]
        if lv in dist:
            dist[lv]["metrics"] += 1
    for r in mapped_attrs:
        lv = r["confidence_level"]
        if lv in dist:
            dist[lv]["attrs"] += 1

    if total_mapped:
        overall_conf = sum(r["confidence"] for r in mapped_metrics + mapped_attrs) / total_mapped
    else:
        overall_conf = 0.0

    # Parity status breakdown
    status_counts: Dict[str, int] = {}
    for r in results:
        s = r.get("parity_status") or "No Status"
        status_counts[s] = status_counts.get(s, 0) + 1

    # ── Build report ───────────────────────────────────────────────────
    lines: List[str] = []

    lines.append("# MSTR → Power BI Migration Mapping Report")
    lines.append(f"> Generated: {now}")
    if scope_label:
        lines.append(f"> Scope: **Focused analysis** — {len(metrics)} metrics, {len(attrs)} attributes from {scope_label}")
    else:
        lines.append(f"> Scope: **Prioritized objects only** — metrics and attributes used by prioritized reports")
    lines.append("")

    # Executive Summary
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(_md_table(
        ["Category", "Total", "Dropped", "In Scope", "Mapped", "Unmapped", "Coverage"],
        [
            ["Metrics", str(len(metrics)), str(len(dropped_m)), str(in_scope_m),
             str(len(mapped_metrics)), str(unmapped_m_count), _pct(len(mapped_metrics), in_scope_m)],
            ["Attributes", str(len(attrs)), str(len(dropped_a)), str(in_scope_a),
             str(len(mapped_attrs)), str(unmapped_a_count), _pct(len(mapped_attrs), in_scope_a)],
            ["**Total**", f"**{total}**", f"**{len(dropped)}**", f"**{in_scope}**",
             f"**{total_mapped}**", f"**{in_scope - total_mapped}**",
             f"**{_pct(total_mapped, in_scope)}**"],
        ],
    ))
    lines.append("")

    # Parity Status Overview
    lines.append("## Parity Status Overview")
    lines.append("")
    status_rows = []
    for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        status_rows.append([s, str(c), _pct(c, total)])
    lines.append(_md_table(["Status", "Count", "% of Total"], status_rows))
    lines.append("")

    # Confidence Distribution
    lines.append("## Confidence Distribution")
    lines.append("")
    dist_rows = []
    for lv in levels:
        t = dist[lv]["metrics"] + dist[lv]["attrs"]
        dist_rows.append([
            lv,
            str(dist[lv]["metrics"]),
            str(dist[lv]["attrs"]),
            str(t),
            _pct(t, total_mapped) if total_mapped else "0.0%",
        ])
    lines.append(_md_table(
        ["Confidence Level", "Metrics", "Attributes", "Total", "% of Mapped"],
        dist_rows,
    ))
    lines.append("")
    lines.append(f"### Overall Confidence Score: {overall_conf:.1%}")
    lines.append("")

    # ── Metrics sections ───────────────────────────────────────────────
    lines.append("---")
    lines.append("")
    lines.append("## Metrics Mapping")
    lines.append("")

    for level in levels:
        level_items = [r for r in metrics if r["confidence_level"] == level and r.get("parity_status") != "Drop"]
        if not level_items:
            continue
        level_items.sort(key=lambda x: (-x["confidence"], -x.get("report_count", 0)))

        label = level
        if level == "Medium":
            label = "Medium — Needs Review"
        elif level == "Low":
            label = "Low — Manual Verification Required"

        lines.append(f"### {label} ({len(level_items)})")
        lines.append("")

        rows = []
        for r in level_items:
            pri = r.get("priority", "")
            pri_str = f"P{int(pri)}" if pri and pri != "" else "-"
            rows.append([
                r["mstr_name"],
                r["pbi_name"],
                r["pbi_model"],
                f"{r['confidence']:.0%}",
                r["signals_used"],
                pri_str,
                r.get("parity_status", ""),
                str(r.get("report_count", 0)),
            ])
        lines.append(_md_table(
            ["MSTR Name", "PBI Name", "PBI Model", "Confidence", "Signals", "Priority", "Status", "Reports"],
            rows,
        ))
        lines.append("")

    # Unmapped metrics (excluding Dropped)
    unmapped_m = [r for r in metrics if r["confidence_level"] == "Unmapped" and r.get("parity_status") != "Drop"]
    dropped_m = [r for r in metrics if r.get("parity_status") == "Drop"]

    if unmapped_m:
        unmapped_m.sort(key=lambda x: -x.get("report_count", 0))
        lines.append(f"### Unmapped Metrics ({len(unmapped_m)})")
        lines.append("")
        rows = []
        for r in unmapped_m:
            pri = r.get("priority", "")
            pri_str = f"P{int(pri)}" if pri and pri != "" else "-"
            rows.append([
                r["mstr_name"],
                r.get("mstr_formula", "") or "",
                r["parity_status"],
                pri_str,
                str(r.get("report_count", 0)),
                r["pbi_name"] if r["pbi_name"] else "-",
                f"{r['confidence']:.0%}" if r["confidence"] > 0 else "-",
            ])
        lines.append(_md_table(
            ["MSTR Name", "Formula", "Status", "Priority", "Reports", "Best Candidate", "Score"],
            rows,
        ))
        lines.append("")

    if dropped_m:
        lines.append(f"### Dropped Metrics — Will Not Migrate ({len(dropped_m)})")
        lines.append("")
        rows = []
        for r in dropped_m:
            rows.append([r["mstr_name"], str(r.get("report_count", 0))])
        lines.append(_md_table(["MSTR Name", "Reports"], rows))
        lines.append("")

    # ── Attributes sections ────────────────────────────────────────────
    lines.append("---")
    lines.append("")
    lines.append("## Attributes Mapping")
    lines.append("")

    for level in levels:
        level_items = [r for r in attrs if r["confidence_level"] == level and r.get("parity_status") != "Drop"]
        if not level_items:
            continue
        level_items.sort(key=lambda x: (-x["confidence"], -x.get("report_count", 0)))

        label = level
        if level == "Medium":
            label = "Medium — Needs Review"
        elif level == "Low":
            label = "Low — Manual Verification Required"

        lines.append(f"### {label} ({len(level_items)})")
        lines.append("")

        rows = []
        for r in level_items:
            pri = r.get("priority", "")
            pri_str = f"P{int(pri)}" if pri and pri != "" else "-"
            rows.append([
                r["mstr_name"],
                r["pbi_name"],
                r["pbi_model"],
                r.get("ade_column", "") or "",
                f"{r['confidence']:.0%}",
                r["signals_used"],
                pri_str,
                r.get("parity_status", ""),
                str(r.get("report_count", 0)),
            ])
        lines.append(_md_table(
            ["MSTR Name", "PBI Name", "PBI Model", "ADE Column", "Confidence", "Signals", "Priority", "Status", "Reports"],
            rows,
        ))
        lines.append("")

    # Unmapped attributes (excluding Dropped)
    unmapped_a = [r for r in attrs if r["confidence_level"] == "Unmapped" and r.get("parity_status") != "Drop"]
    dropped_a = [r for r in attrs if r.get("parity_status") == "Drop"]

    if unmapped_a:
        unmapped_a.sort(key=lambda x: -x.get("report_count", 0))
        lines.append(f"### Unmapped Attributes ({len(unmapped_a)})")
        lines.append("")
        rows = []
        for r in unmapped_a:
            pri = r.get("priority", "")
            pri_str = f"P{int(pri)}" if pri and pri != "" else "-"
            rows.append([
                r["mstr_name"],
                r.get("edw_column", "") or "",
                r.get("ade_column", "") or "",
                r["parity_status"],
                pri_str,
                str(r.get("report_count", 0)),
            ])
        lines.append(_md_table(
            ["MSTR Name", "EDW Column", "ADE Column", "Status", "Priority", "Reports"],
            rows,
        ))
        lines.append("")

    if dropped_a:
        lines.append(f"### Dropped Attributes — Will Not Migrate ({len(dropped_a)})")
        lines.append("")
        rows = []
        for r in dropped_a:
            rows.append([r["mstr_name"], str(r.get("report_count", 0))])
        lines.append(_md_table(["MSTR Name", "Reports"], rows))
        lines.append("")

    # ── Migration Risk Analysis ────────────────────────────────────────
    lines.append("---")
    lines.append("")
    lines.append("## Migration Risk Analysis")
    lines.append("")

    # N-to-1 merges: multiple MSTR -> same PBI name
    pbi_name_counts: Dict[str, List[str]] = {}
    for r in results:
        if r["pbi_name"] and r["confidence_level"] != "Unmapped":
            pbi_name_counts.setdefault(r["pbi_name"], []).append(r["mstr_name"])
    n_to_1 = {k: v for k, v in pbi_name_counts.items() if len(v) > 1}

    planned = [r for r in results if r["parity_status"] in ("Planned",)]
    not_planned = [r for r in results if r["parity_status"] in ("Not Planned",)]
    drop_all = [r for r in results if r["parity_status"] in ("Drop",)]

    risk_rows = [
        ["N-to-1 merges (multiple MSTR → one PBI)", str(len(n_to_1)),
         f"e.g. {list(n_to_1.keys())[:5]}" if n_to_1 else "-"],
        ["Planned (migration in progress)", str(len(planned)), "parity_status = Planned"],
        ["Dropped (will not migrate)", str(len(drop_all)), "parity_status = Drop"],
        ["Not Planned", str(len(not_planned)), "parity_status = Not Planned"],
        ["Unmapped Metrics (excl. Drop)", str(len(unmapped_m)), "No matching PBI measure found"],
        ["Unmapped Attributes (excl. Drop)", str(len(unmapped_a)), "No matching PBI column found"],
    ]
    lines.append(_md_table(["Risk Area", "Count", "Details"], risk_rows))
    lines.append("")

    # N-to-1 detail
    if n_to_1:
        lines.append(f"### N-to-1 Merge Details ({len(n_to_1)} total)")
        lines.append("")
        merge_rows = []
        for pbi_name, mstr_names in sorted(n_to_1.items(), key=lambda x: -len(x[1])):
            merge_rows.append([
                pbi_name,
                str(len(mstr_names)),
                ", ".join(mstr_names),
            ])
        lines.append(_md_table(["PBI Name", "MSTR Count", "MSTR Names"], merge_rows))
        lines.append("")

    # Write report
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"mapping-report-{timestamp}.md"
    filepath = os.path.join(output_dir, filename)

    report_text = "\n".join(lines)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(report_text)

    # Also write a "latest" symlink / copy
    latest_path = os.path.join(output_dir, "mapping-report-latest.md")
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(f"\n  Report written to: {filepath}")
    print(f"  Latest copy at:    {latest_path}")
    return filepath


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="MSTR → PBI Migration Mapping Tool")
    parser.add_argument(
        "--output", "-o",
        default=config.OUTPUT_DIR,
        help="Output directory for the mapping report",
    )
    parser.add_argument(
        "--filter", "-f",
        default=None,
        help="Comma-separated MSTR object names to analyse (focused mode)",
    )
    parser.add_argument(
        "--filter-file",
        default=None,
        help="Path to a JSON file with metric/attribute names to analyse (focused mode)",
    )
    parser.add_argument(
        "--scope-label",
        default=None,
        help="Label for the scope shown in the report header (e.g. 'Heart Dashboard KPIs')",
    )
    args = parser.parse_args()

    filter_names = _load_filter_names(args.filter, args.filter_file)
    scope_label = args.scope_label

    if filter_names and not scope_label:
        scope_label = "user-specified filter"

    results = run_pipeline(filter_names=filter_names, scope_label=scope_label)

    # Phase 3: Generate report
    print(f"\n[Phase 3] Generating Markdown report...")
    report_path = generate_report(results, args.output, scope_label=scope_label)

    # Summary
    mapped = [r for r in results if r["confidence_level"] != "Unmapped"]
    count = len(results) or 1
    print(f"\n{'=' * 60}")
    print(f"DONE — {len(mapped)}/{len(results)} objects mapped ({len(mapped)/count*100:.1f}%)")
    if scope_label:
        print(f"Scope: {scope_label}")
    print(f"Report: {report_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
