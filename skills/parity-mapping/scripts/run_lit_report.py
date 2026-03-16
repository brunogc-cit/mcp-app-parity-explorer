#!/usr/bin/env python3
"""
Run parity mapping scoped to the [Supply Chain] LIT Report only.

Filters the MSTR cache to include only the 19 objects (3 metrics + 16 attributes)
that belong to the LIT Report (GUID: 2806F1C6498B9D75CF949E923C00AF51),
then runs the standard mapping pipeline.
"""

import json
import os
import sys

_TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TOOL_DIR)

import config
import run_mapping

LIT_REPORT_GUIDS = {
    "3A9EA053431B0B07EDF64CB90E8B02B9",  # Billed Date (Attribute)
    "02639F6544C8A5D8C1628680F3B8B05E",  # Carrier Method (Attribute)
    "E76C86E644B2EF7BDD101BA8B671593F",  # Customer Account (Attribute)
    "C72D65134127841F3E6C039C04718519",  # Customer Account Email (Attribute)
    "B052657F44F12B4010679A85B28D020E",  # Customer Shipping Address (Attribute)
    "9A861CE943F28855BDB32BA2687CCD3A",  # Delivery City (Attribute)
    "68530B09402ADA2751C6A4B059CDA5BF",  # Delivery Country (Attribute)
    "7ACF5B674C8B7C0430343CAC8EE1EA0C",  # Delivery Postcode (Attribute)
    "3BE53F3A4E53C5F6D9230998771F18EF",  # Latest Parcel Delivery Status (Attribute)
    "0BE1AFDF42348EF7E1442490AB66DAD8",  # Latest Void Date (Attribute)
    "DB0DB27D4AC3D8E342E6DE998A8DF5CA",  # Parcel (Attribute)
    "DB3E80BC4DF52EBB73B9AA8F887C93C9",  # Receipt ID (Attribute)
    "9244DA4B44AE09F97DE2FEA4008D59A6",  # Shipped Date (Attribute)
    "3E4CF1B24E65660CC01DE887FF033C5C",  # Shipping Method (Attribute)
    "96C91767430C154E428672BC5D96E34B",  # Void Reason (Attribute)
    "C492DE644B49FE77E632E08C60E1298D",  # Warehouse (Attribute)
    "3F19E5DB4983D521420E34BA3AD7C668",  # Billed Sales Unit Cost Value (Metric)
    "42CF34E1466D442A69683FAC52CC052A",  # Billed Sales Value (Metric)
    "51CBCE00489CDC7EC235E0860E60B410",  # Void Sales Units (Metric)
}


def create_filtered_cache():
    """Filter the full MSTR cache to only LIT Report objects."""
    with open(config.CACHE_PATH, encoding="utf-8") as f:
        all_items = json.load(f)

    filtered = [item for item in all_items if item.get("guid") in LIT_REPORT_GUIDS]

    found_guids = {item["guid"] for item in filtered}
    missing = LIT_REPORT_GUIDS - found_guids
    if missing:
        print(f"  WARNING: {len(missing)} GUIDs not found in cache: {missing}")

    return filtered


def main():
    import extract_pbi
    import extract_dbt
    import signals

    print("=" * 60)
    print("MSTR → PBI Migration Mapping Tool")
    print("SCOPED: [Supply Chain] LIT Report")
    print("=" * 60)

    # Phase 1: Extract (filtered MSTR, full PBI/dbt)
    print("\n[Phase 1] Extracting data sources...")
    print("  Filtering MSTR cache to LIT Report objects...")
    mstr_objects = create_filtered_cache()
    metrics = [o for o in mstr_objects if o.get("type") == "Metric"]
    attrs = [o for o in mstr_objects if o.get("type") == "Attribute"]
    print(f"  MSTR (filtered): {len(metrics)} metrics, {len(attrs)} attributes")

    pbi_data = extract_pbi.extract_all_models()
    pbi_measures = extract_pbi.build_measure_index(pbi_data)
    pbi_columns = extract_pbi.build_column_index(pbi_data)

    dbt_columns = extract_dbt.extract_serve_columns()

    pbi_table_sources = {}
    for model_name, model in pbi_data.get("models", {}).items():
        for table_name, table in model.get("tables", {}).items():
            if table.get("source_table"):
                key = f"{model_name}/{table_name}"
                fqn = f"{table.get('source_catalog','')}.{table.get('source_schema','')}.{table['source_table']}"
                pbi_table_sources[key] = fqn

    print(f"\n  PBI targets: {len(pbi_measures)} measures, {len(pbi_columns)} columns")
    print(f"  DBT serve: {len(dbt_columns)} tables")
    print(f"  PBI table sources: {len(pbi_table_sources)} entries")

    # Phase 2: Score
    print(f"\n[Phase 2] Scoring {len(mstr_objects)} MSTR objects...")
    results = []
    for obj in mstr_objects:
        r = run_mapping.score_object(obj, pbi_measures, pbi_columns, pbi_table_sources)
        results.append(r)
    print(f"  Scoring complete: {len(results)} objects processed")

    # Phase 3: Generate report
    output_dir = os.path.join(_TOOL_DIR, "output", "lit-report")
    print(f"\n[Phase 3] Generating Markdown report...")
    report_path = run_mapping.generate_report(results, output_dir)

    mapped = [r for r in results if r["confidence_level"] != "Unmapped"]
    print(f"\n{'=' * 60}")
    print(f"DONE — {len(mapped)}/{len(results)} objects mapped ({len(mapped)/len(results)*100:.1f}%)")
    print(f"Report: {report_path}")
    print(f"{'=' * 60}")

    return report_path


if __name__ == "__main__":
    main()
