#!/usr/bin/env python3
"""
Batch parity mapping for Supply Chain reports.

Runs the MSTR-to-PBI parity mapping algorithm scoped to each report,
then generates a consolidated HTML report (convertible to PDF).
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

_TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TOOL_DIR)

import config
import extract_pbi
import extract_dbt
import run_mapping

# ═══════════════════════════════════════════════════════════════════════════
# Report definitions — ticket, display name, Neo4j name, GUID, object GUIDs
# ═══════════════════════════════════════════════════════════════════════════

REPORTS = [
    {
        "ticket": "1176946",
        "display_name": "LIT Report",
        "neo4j_name": "LIT Report",
        "guid": "2806F1C6498B9D75CF949E923C00AF51",
        "obj_guids": {
            "3A9EA053431B0B07EDF64CB90E8B02B9", "02639F6544C8A5D8C1628680F3B8B05E",
            "E76C86E644B2EF7BDD101BA8B671593F", "C72D65134127841F3E6C039C04718519",
            "B052657F44F12B4010679A85B28D020E", "9A861CE943F28855BDB32BA2687CCD3A",
            "68530B09402ADA2751C6A4B059CDA5BF", "7ACF5B674C8B7C0430343CAC8EE1EA0C",
            "3BE53F3A4E53C5F6D9230998771F18EF", "0BE1AFDF42348EF7E1442490AB66DAD8",
            "DB0DB27D4AC3D8E342E6DE998A8DF5CA", "DB3E80BC4DF52EBB73B9AA8F887C93C9",
            "9244DA4B44AE09F97DE2FEA4008D59A6", "3E4CF1B24E65660CC01DE887FF033C5C",
            "96C91767430C154E428672BC5D96E34B", "C492DE644B49FE77E632E08C60E1298D",
            "3F19E5DB4983D521420E34BA3AD7C668", "42CF34E1466D442A69683FAC52CC052A",
            "51CBCE00489CDC7EC235E0860E60B410",
        },
    },
    {
        "ticket": "1175093",
        "display_name": "Forecast - Day Report",
        "neo4j_name": "Forecast - Day Report",
        "guid": "1429817840F7953173F9B0921DC7AB5F",
        "obj_guids": {
            "2F00974D44E1D0D24CA344ABD872806A", "68530B09402ADA2751C6A4B059CDA5BF",
            "69512ABC47FF60733286039A929C6E39", "10DF15E14E455D43C4ACB4A458A0FA7E",
            "F8899EB2461180B76A90FAA4BF026EB1", "EA1E64B34687DF3A731B0F9CEB05AADB",
            "C492DE644B49FE77E632E08C60E1298D", "96CA79BF4CA895886260078FA99DBDEF",
            "FCCA014F4A1053C375290E9BE52BC640", "E50193A144D3BBAA4B8C938EBFBDA01A",
        },
    },
    {
        "ticket": "1175093",
        "display_name": "Forecast - Hour Report",
        "neo4j_name": "Forecast - Hour Report",
        "guid": "B95D5949489E4166EFA5DA81770DBBE3",
        "obj_guids": {
            "2F00974D44E1D0D24CA344ABD872806A", "68530B09402ADA2751C6A4B059CDA5BF",
            "69512ABC47FF60733286039A929C6E39", "10DF15E14E455D43C4ACB4A458A0FA7E",
            "D68B31E84C82CC8810B0488DE5132A1D", "F8899EB2461180B76A90FAA4BF026EB1",
            "EA1E64B34687DF3A731B0F9CEB05AADB", "C492DE644B49FE77E632E08C60E1298D",
            "96CA79BF4CA895886260078FA99DBDEF", "FCCA014F4A1053C375290E9BE52BC640",
        },
    },
    {
        "ticket": "1175093",
        "display_name": "Forecast - Carrier Report",
        "neo4j_name": "Forecast - Carrier Report",
        "guid": "54D07A314F1FEED45630A486652BF37F",
        "obj_guids": {
            "02639F6544C8A5D8C1628680F3B8B05E", "2F00974D44E1D0D24CA344ABD872806A",
            "68530B09402ADA2751C6A4B059CDA5BF", "69512ABC47FF60733286039A929C6E39",
            "10DF15E14E455D43C4ACB4A458A0FA7E", "F8899EB2461180B76A90FAA4BF026EB1",
            "EA1E64B34687DF3A731B0F9CEB05AADB", "C492DE644B49FE77E632E08C60E1298D",
            "96CA79BF4CA895886260078FA99DBDEF", "FCCA014F4A1053C375290E9BE52BC640",
        },
    },
    {
        "ticket": "1176929",
        "display_name": "IIR/MFO Weekly",
        "neo4j_name": "IIR Weekly",
        "guid": "D9084B11427A5DF244C22A814EE360F5",
        "obj_guids": {
            "355A71E1429D7AA098A05AA991CCA216", "7D84152D46EF6D3F6EB5268A75611624",
            "DE41C0244B6F35286E9F659960B856D5", "C84A377143CFBF84F543C2852765961D",
            "96C91767430C154E428672BC5D96E34B", "67AE0F024F62BCA3F2B6F0B79BCBC3C1",
            "51CBCE00489CDC7EC235E0860E60B410",
        },
    },
    {
        "ticket": "1176929",
        "display_name": "IIR Global",
        "neo4j_name": "IIR report Global",
        "guid": "5C0BDD86467EE9501500F1AA8F1F69D4",
        "obj_guids": {
            "98D4457848E9548C4DF04D81D8F4D22D", "D552FA3F4975AEF25C0D30A25B83B7FD",
            "DE41C0244B6F35286E9F659960B856D5", "C84A377143CFBF84F543C2852765961D",
            "F585A9674B25FF3BE78924AEA5ED55C8", "96C91767430C154E428672BC5D96E34B",
            "51CBCE00489CDC7EC235E0860E60B410",
        },
    },
    {
        "ticket": "1176929",
        "display_name": "SHIPPED VS RETURNS",
        "neo4j_name": "Shipped vs Returns",
        "guid": "2B3FA8914754113478118DA5CC78050A",
        "obj_guids": {
            "DE41C0244B6F35286E9F659960B856D5", "C84A377143CFBF84F543C2852765961D",
            "815B38AB4966AD486BAD53BEDCE0607F", "65599CAC4943A30411766F8328C6D65E",
            "67AE0F024F62BCA3F2B6F0B79BCBC3C1",
        },
    },
    {
        "ticket": "1176937",
        "display_name": "Monthly Void Monitoring",
        "neo4j_name": "Monthly void GLOBAL (MBR)",
        "guid": "2602CE0F41244B90D09239B081762836",
        "obj_guids": {
            "7D84152D46EF6D3F6EB5268A75611624", "DE41C0244B6F35286E9F659960B856D5",
            "96C91767430C154E428672BC5D96E34B", "65599CAC4943A30411766F8328C6D65E",
        },
    },
]


def filter_cache(obj_guids: Set[str]) -> List[Dict]:
    with open(config.CACHE_PATH, encoding="utf-8") as f:
        all_items = json.load(f)
    return [item for item in all_items if item.get("guid") in obj_guids]


def run_single_report(report: Dict, pbi_measures, pbi_columns, pbi_table_sources) -> Dict[str, Any]:
    """Run parity mapping for a single report and return structured results."""
    name = report["display_name"]
    print(f"\n  [{report['ticket']}] {name}...")

    mstr_objects = filter_cache(report["obj_guids"])
    metrics_in = [o for o in mstr_objects if o.get("type") == "Metric"]
    attrs_in = [o for o in mstr_objects if o.get("type") == "Attribute"]
    print(f"    Cache: {len(metrics_in)}M + {len(attrs_in)}A = {len(mstr_objects)} objects")

    results = []
    for obj in mstr_objects:
        r = run_mapping.score_object(obj, pbi_measures, pbi_columns, pbi_table_sources)
        results.append(r)

    metrics = [r for r in results if r["mstr_type"] == "Metric"]
    attrs = [r for r in results if r["mstr_type"] == "Attribute"]
    dropped = [r for r in results if r.get("parity_status") == "Drop"]
    mapped = [r for r in results if r["confidence_level"] != "Unmapped" and r.get("parity_status") != "Drop"]
    in_scope = len(results) - len(dropped)
    mapped_m = [r for r in metrics if r["confidence_level"] != "Unmapped" and r.get("parity_status") != "Drop"]
    mapped_a = [r for r in attrs if r["confidence_level"] != "Unmapped" and r.get("parity_status") != "Drop"]
    dropped_m = [r for r in dropped if r["mstr_type"] == "Metric"]
    dropped_a = [r for r in dropped if r["mstr_type"] == "Attribute"]

    levels = ["Confirmed", "High", "Medium", "Low"]
    dist = {lv: 0 for lv in levels}
    for r in mapped:
        lv = r["confidence_level"]
        if lv in dist:
            dist[lv] += 1

    overall_conf = sum(r["confidence"] for r in mapped) / len(mapped) if mapped else 0.0

    coverage = len(mapped) / in_scope * 100 if in_scope else 0.0
    print(f"    Result: {len(mapped)}/{in_scope} mapped ({coverage:.1f}%), confidence {overall_conf:.1%}")

    return {
        "ticket": report["ticket"],
        "display_name": name,
        "neo4j_name": report["neo4j_name"],
        "guid": report["guid"],
        "total": len(results),
        "metrics_total": len(metrics),
        "attrs_total": len(attrs),
        "dropped": len(dropped),
        "dropped_m": len(dropped_m),
        "dropped_a": len(dropped_a),
        "in_scope": in_scope,
        "mapped": len(mapped),
        "mapped_m": len(mapped_m),
        "mapped_a": len(mapped_a),
        "unmapped": in_scope - len(mapped),
        "coverage_pct": coverage,
        "overall_confidence": overall_conf,
        "distribution": dist,
        "results": results,
    }


def generate_html_report(all_reports: List[Dict[str, Any]], output_path: str) -> str:
    """Generate a beautiful HTML report."""
    now = datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")

    grand_total = sum(r["total"] for r in all_reports)
    grand_dropped = sum(r["dropped"] for r in all_reports)
    grand_in_scope = sum(r["in_scope"] for r in all_reports)
    grand_mapped = sum(r["mapped"] for r in all_reports)
    grand_unmapped = grand_in_scope - grand_mapped
    grand_coverage = grand_mapped / grand_in_scope * 100 if grand_in_scope else 0
    grand_conf_sum = sum(r["overall_confidence"] * r["mapped"] for r in all_reports if r["mapped"])
    grand_conf = grand_conf_sum / grand_mapped if grand_mapped else 0

    def _pct(n, d):
        return f"{n/d*100:.1f}%" if d else "0%"

    def _conf_color(conf):
        if conf >= 0.9: return "#10b981"
        if conf >= 0.7: return "#3b82f6"
        if conf >= 0.5: return "#f59e0b"
        if conf >= 0.3: return "#ef4444"
        return "#6b7280"

    def _coverage_color(pct):
        if pct >= 90: return "#10b981"
        if pct >= 70: return "#3b82f6"
        if pct >= 50: return "#f59e0b"
        return "#ef4444"

    def _level_badge(level):
        colors = {
            "Confirmed": ("#dcfce7", "#166534"),
            "High": ("#dbeafe", "#1e40af"),
            "Medium": ("#fef3c7", "#92400e"),
            "Low": ("#fee2e2", "#991b1b"),
            "Unmapped": ("#f3f4f6", "#374151"),
        }
        bg, fg = colors.get(level, ("#f3f4f6", "#374151"))
        return f'<span style="background:{bg};color:{fg};padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600;">{level}</span>'

    def _status_badge(status):
        colors = {
            "Complete": ("#dcfce7", "#166534"),
            "Planned": ("#dbeafe", "#1e40af"),
            "Drop": ("#fee2e2", "#991b1b"),
            "Not Planned": ("#f3f4f6", "#374151"),
            "No Status": ("#f3f4f6", "#6b7280"),
        }
        bg, fg = colors.get(status, ("#f3f4f6", "#374151"))
        return f'<span style="background:{bg};color:{fg};padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:500;">{status}</span>'

    # Build per-report detail sections
    report_sections = []
    for idx, rpt in enumerate(all_reports):
        results = rpt["results"]
        metrics = sorted([r for r in results if r["mstr_type"] == "Metric"], key=lambda x: -x["confidence"])
        attrs = sorted([r for r in results if r["mstr_type"] == "Attribute"], key=lambda x: -x["confidence"])

        def _obj_rows(items):
            rows = []
            for r in items:
                conf = r["confidence"]
                level = r["confidence_level"]
                status = r.get("parity_status") or "No Status"
                pbi = r["pbi_name"] or '<span style="color:#9ca3af;">--</span>'
                model = r["pbi_model"] or ""
                signals = r["signals_used"] or ""
                rows.append(f"""<tr>
                    <td style="font-weight:500;">{r["mstr_name"]}</td>
                    <td>{pbi}</td>
                    <td style="font-size:12px;color:#6b7280;">{model}</td>
                    <td style="text-align:center;">{_level_badge(level)}</td>
                    <td style="text-align:center;font-weight:600;color:{_conf_color(conf)};">{conf:.0%}</td>
                    <td style="text-align:center;font-size:12px;color:#6b7280;">{signals}</td>
                    <td style="text-align:center;">{_status_badge(status)}</td>
                </tr>""")
            return "\n".join(rows)

        cov_color = _coverage_color(rpt["coverage_pct"])
        conf_color = _conf_color(rpt["overall_confidence"])

        section = f"""
        <div class="report-card" id="report-{idx}">
            <div class="report-header">
                <div style="display:flex;align-items:center;gap:12px;">
                    <span class="ticket-badge">#{rpt["ticket"]}</span>
                    <h2 style="margin:0;font-size:20px;">{rpt["display_name"]}</h2>
                </div>
                <div style="font-size:12px;color:#6b7280;">Neo4j: {rpt["neo4j_name"]} &middot; {rpt["guid"][:12]}...</div>
            </div>
            <div class="kpi-row">
                <div class="kpi">
                    <div class="kpi-value">{rpt["total"]}</div>
                    <div class="kpi-label">Total Objects</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value">{rpt["metrics_total"]}M / {rpt["attrs_total"]}A</div>
                    <div class="kpi-label">Metrics / Attributes</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value" style="color:{cov_color};">{rpt["coverage_pct"]:.1f}%</div>
                    <div class="kpi-label">Coverage ({rpt["mapped"]}/{rpt["in_scope"]})</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value" style="color:{conf_color};">{rpt["overall_confidence"]:.1%}</div>
                    <div class="kpi-label">Avg Confidence</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value conf-dist">
                        <span style="color:#166534;">{rpt["distribution"]["Confirmed"]}</span> /
                        <span style="color:#1e40af;">{rpt["distribution"]["High"]}</span> /
                        <span style="color:#92400e;">{rpt["distribution"]["Medium"]}</span> /
                        <span style="color:#991b1b;">{rpt["distribution"]["Low"]}</span>
                    </div>
                    <div class="kpi-label">C / H / M / L</div>
                </div>
            </div>
            <div class="coverage-bar-container">
                <div class="coverage-bar" style="width:{rpt['coverage_pct']:.1f}%;background:{cov_color};"></div>
            </div>
            """

        if metrics:
            section += f"""
            <h3 style="margin:20px 0 8px 0;font-size:15px;color:#374151;">Metrics ({len(metrics)})</h3>
            <table class="mapping-table">
                <thead><tr>
                    <th>MSTR Name</th><th>PBI Name</th><th>Model</th><th>Level</th><th>Conf.</th><th>Signals</th><th>Status</th>
                </tr></thead>
                <tbody>{_obj_rows(metrics)}</tbody>
            </table>"""

        if attrs:
            section += f"""
            <h3 style="margin:20px 0 8px 0;font-size:15px;color:#374151;">Attributes ({len(attrs)})</h3>
            <table class="mapping-table">
                <thead><tr>
                    <th>MSTR Name</th><th>PBI Name</th><th>Model</th><th>Level</th><th>Conf.</th><th>Signals</th><th>Status</th>
                </tr></thead>
                <tbody>{_obj_rows(attrs)}</tbody>
            </table>"""

        section += "\n        </div>"
        report_sections.append(section)

    # Build summary table
    summary_rows = []
    for rpt in all_reports:
        cov_color = _coverage_color(rpt["coverage_pct"])
        conf_color = _conf_color(rpt["overall_confidence"])
        summary_rows.append(f"""<tr>
            <td><span class="ticket-badge-sm">#{rpt["ticket"]}</span></td>
            <td style="font-weight:500;">{rpt["display_name"]}</td>
            <td style="text-align:center;">{rpt["total"]}</td>
            <td style="text-align:center;">{rpt["metrics_total"]}M / {rpt["attrs_total"]}A</td>
            <td style="text-align:center;">{rpt["dropped"]}</td>
            <td style="text-align:center;">{rpt["mapped"]}/{rpt["in_scope"]}</td>
            <td style="text-align:center;font-weight:700;color:{cov_color};">{rpt["coverage_pct"]:.1f}%</td>
            <td style="text-align:center;font-weight:700;color:{conf_color};">{rpt["overall_confidence"]:.1%}</td>
            <td style="text-align:center;font-size:12px;">
                <span style="color:#166534;">{rpt["distribution"]["Confirmed"]}</span> /
                <span style="color:#1e40af;">{rpt["distribution"]["High"]}</span> /
                <span style="color:#92400e;">{rpt["distribution"]["Medium"]}</span> /
                <span style="color:#991b1b;">{rpt["distribution"]["Low"]}</span>
            </td>
        </tr>""")

    # Aggregate confidence distribution
    agg_dist = {"Confirmed": 0, "High": 0, "Medium": 0, "Low": 0}
    for rpt in all_reports:
        for lv in agg_dist:
            agg_dist[lv] += rpt["distribution"][lv]

    # Unique tickets
    tickets = sorted(set(r["ticket"] for r in all_reports))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Supply Chain — MSTR to PBI Parity Mapping Report</title>
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background: #f8fafc; color: #1e293b; line-height: 1.5; }}
    @page {{ size: landscape; margin: 12mm; }}
    @media print {{
        body {{ background: white; }}
        .report-card {{ break-inside: avoid; }}
        .page-break {{ page-break-before: always; }}
    }}
    .container {{ max-width: 1100px; margin: 0 auto; padding: 40px 24px; }}
    .hero {{ background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #0f172a 100%); color: white; padding: 48px 40px; border-radius: 16px; margin-bottom: 32px; position: relative; overflow: hidden; }}
    .hero::before {{ content: ''; position: absolute; top: -50%; right: -20%; width: 500px; height: 500px; background: radial-gradient(circle, rgba(59,130,246,0.15) 0%, transparent 70%); }}
    .hero h1 {{ font-size: 28px; font-weight: 800; margin-bottom: 4px; letter-spacing: -0.5px; }}
    .hero .subtitle {{ font-size: 16px; color: #94a3b8; font-weight: 400; }}
    .hero .meta {{ margin-top: 20px; display: flex; gap: 24px; font-size: 13px; color: #64748b; }}
    .hero .meta span {{ display: flex; align-items: center; gap: 6px; }}

    .grand-kpis {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 32px; }}
    .grand-kpi {{ background: white; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid #e2e8f0; }}
    .grand-kpi .value {{ font-size: 32px; font-weight: 800; letter-spacing: -1px; }}
    .grand-kpi .label {{ font-size: 12px; color: #64748b; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }}

    .section-title {{ font-size: 18px; font-weight: 700; color: #0f172a; margin: 32px 0 16px 0; display: flex; align-items: center; gap: 8px; }}
    .section-title::before {{ content: ''; display: block; width: 4px; height: 20px; background: #3b82f6; border-radius: 2px; }}

    .summary-table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid #e2e8f0; }}
    .summary-table th {{ background: #f8fafc; padding: 12px 14px; text-align: left; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; border-bottom: 2px solid #e2e8f0; }}
    .summary-table td {{ padding: 12px 14px; border-bottom: 1px solid #f1f5f9; font-size: 13px; }}
    .summary-table tbody tr:last-child td {{ border-bottom: none; }}
    .summary-table tbody tr:hover {{ background: #f8fafc; }}

    .report-card {{ background: white; border-radius: 12px; padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid #e2e8f0; }}
    .report-header {{ margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid #f1f5f9; }}
    .ticket-badge {{ background: #eff6ff; color: #1d4ed8; padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; }}
    .ticket-badge-sm {{ background: #eff6ff; color: #1d4ed8; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; }}

    .kpi-row {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 12px; }}
    .kpi {{ text-align: center; padding: 10px; background: #f8fafc; border-radius: 8px; }}
    .kpi-value {{ font-size: 20px; font-weight: 700; letter-spacing: -0.5px; }}
    .kpi-value.conf-dist {{ font-size: 15px; }}
    .kpi-label {{ font-size: 11px; color: #64748b; font-weight: 500; margin-top: 2px; }}

    .coverage-bar-container {{ height: 6px; background: #e2e8f0; border-radius: 3px; overflow: hidden; margin-bottom: 4px; }}
    .coverage-bar {{ height: 100%; border-radius: 3px; transition: width 0.5s ease; }}

    .mapping-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    .mapping-table th {{ background: #f8fafc; padding: 8px 10px; text-align: left; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.4px; color: #64748b; border-bottom: 2px solid #e2e8f0; }}
    .mapping-table td {{ padding: 8px 10px; border-bottom: 1px solid #f1f5f9; }}
    .mapping-table tbody tr:hover {{ background: #fafbfd; }}

    .footer {{ text-align: center; padding: 32px 0; font-size: 12px; color: #94a3b8; }}
    .footer a {{ color: #3b82f6; text-decoration: none; }}
</style>
</head>
<body>
<div class="container">

    <div class="hero">
        <h1>MSTR &rarr; Power BI Parity Mapping</h1>
        <div class="subtitle">Supply Chain Reports &mdash; Migration Coverage Analysis</div>
        <div class="meta">
            <span>Generated: {now}</span>
            <span>Reports: {len(all_reports)}</span>
            <span>Tickets: {', '.join(tickets)}</span>
        </div>
    </div>

    <div class="grand-kpis">
        <div class="grand-kpi">
            <div class="value">{len(all_reports)}</div>
            <div class="label">Reports Analysed</div>
        </div>
        <div class="grand-kpi">
            <div class="value">{grand_total}</div>
            <div class="label">Total Objects</div>
        </div>
        <div class="grand-kpi">
            <div class="value" style="color:{_coverage_color(grand_coverage)};">{grand_coverage:.1f}%</div>
            <div class="label">Overall Coverage</div>
        </div>
        <div class="grand-kpi">
            <div class="value" style="color:{_conf_color(grand_conf)};">{grand_conf:.1%}</div>
            <div class="label">Avg Confidence</div>
        </div>
        <div class="grand-kpi">
            <div class="value" style="font-size:18px;">
                <span style="color:#166534;">{agg_dist["Confirmed"]}</span> /
                <span style="color:#1e40af;">{agg_dist["High"]}</span> /
                <span style="color:#92400e;">{agg_dist["Medium"]}</span> /
                <span style="color:#991b1b;">{agg_dist["Low"]}</span>
            </div>
            <div class="label">C / H / M / L Distribution</div>
        </div>
    </div>

    <div class="section-title">Summary by Report</div>
    <table class="summary-table">
        <thead><tr>
            <th>Ticket</th><th>Report Name</th><th>Total</th><th>M / A</th><th>Drop</th><th>Mapped</th><th>Coverage</th><th>Confidence</th><th>C / H / M / L</th>
        </tr></thead>
        <tbody>
            {"".join(summary_rows)}
            <tr style="background:#f8fafc;font-weight:600;">
                <td></td>
                <td>Grand Total</td>
                <td style="text-align:center;">{grand_total}</td>
                <td style="text-align:center;">&mdash;</td>
                <td style="text-align:center;">{grand_dropped}</td>
                <td style="text-align:center;">{grand_mapped}/{grand_in_scope}</td>
                <td style="text-align:center;color:{_coverage_color(grand_coverage)};">{grand_coverage:.1f}%</td>
                <td style="text-align:center;color:{_conf_color(grand_conf)};">{grand_conf:.1%}</td>
                <td style="text-align:center;font-size:12px;">
                    <span style="color:#166534;">{agg_dist["Confirmed"]}</span> /
                    <span style="color:#1e40af;">{agg_dist["High"]}</span> /
                    <span style="color:#92400e;">{agg_dist["Medium"]}</span> /
                    <span style="color:#991b1b;">{agg_dist["Low"]}</span>
                </td>
            </tr>
        </tbody>
    </table>

    <div class="section-title" style="margin-top:40px;">Detailed Report Analysis</div>
    {"".join(report_sections)}

    <div class="footer">
        ASOS Data Migration &mdash; Parity Mapping Analyst &middot; Powered by CI&T Flow<br>
        <span style="margin-top:4px;display:inline-block;">Generated on {now}</span>
    </div>

</div>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  HTML report: {output_path}")
    return output_path


def main():
    print("=" * 70)
    print("  MSTR → PBI Parity Mapping — Supply Chain Batch Analysis")
    print("=" * 70)

    # Phase 1: Load shared PBI/dbt data (expensive, do once)
    print("\n[Phase 1] Loading PBI & DBT data sources...")
    pbi_data = extract_pbi.extract_all_models()
    pbi_measures = extract_pbi.build_measure_index(pbi_data)
    pbi_columns = extract_pbi.build_column_index(pbi_data)
    dbt_columns = extract_dbt.extract_serve_columns()

    pbi_table_sources: Dict[str, str] = {}
    for model_name, model in pbi_data.get("models", {}).items():
        for table_name, table in model.get("tables", {}).items():
            if table.get("source_table"):
                key = f"{model_name}/{table_name}"
                fqn = f"{table.get('source_catalog','')}.{table.get('source_schema','')}.{table['source_table']}"
                pbi_table_sources[key] = fqn

    print(f"  PBI: {len(pbi_measures)} measures, {len(pbi_columns)} columns")
    print(f"  DBT: {len(dbt_columns)} tables, PBI sources: {len(pbi_table_sources)}")

    # Phase 2: Run mapping per report
    print(f"\n[Phase 2] Running parity mapping for {len(REPORTS)} reports...")
    all_results = []
    for rpt in REPORTS:
        result = run_single_report(rpt, pbi_measures, pbi_columns, pbi_table_sources)
        all_results.append(result)

    # Phase 3: Generate HTML report
    print(f"\n[Phase 3] Generating HTML report...")
    output_dir = os.path.join(_TOOL_DIR, "output", "supply-chain-batch")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = os.path.join(output_dir, f"parity-mapping-supply-chain-{timestamp}.html")
    generate_html_report(all_results, html_path)

    # Also save as latest
    latest_path = os.path.join(output_dir, "parity-mapping-supply-chain-latest.html")
    with open(html_path, encoding="utf-8") as f:
        content = f.read()
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Latest copy: {latest_path}")

    # Summary
    grand_mapped = sum(r["mapped"] for r in all_results)
    grand_scope = sum(r["in_scope"] for r in all_results)
    print(f"\n{'=' * 70}")
    print(f"  DONE — {len(REPORTS)} reports processed")
    print(f"  Grand total: {grand_mapped}/{grand_scope} objects mapped ({grand_mapped/grand_scope*100:.1f}%)")
    print(f"  HTML report: {html_path}")
    print(f"{'=' * 70}")

    return html_path


if __name__ == "__main__":
    main()
