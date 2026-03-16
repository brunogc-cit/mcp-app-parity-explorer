"""
Matching signals S1-S5 for MSTR-to-PBI mapping.

S1: Direct Neo4j mapping (pb_semantic_name / updated_pb_semantic_name)
S2: Column lineage (ade_db_column -> PBI sourceColumn)
S3: Name similarity (fuzzy matching with domain-specific rules)
S4: Formula / DAX structural analysis
S5: Table context overlap
"""

import re
from typing import Dict, List, Optional, Tuple

try:
    from rapidfuzz import fuzz as rf_fuzz
    from rapidfuzz.distance import Levenshtein as rf_lev

    def _levenshtein_ratio(a: str, b: str) -> float:
        if not a and not b:
            return 1.0
        return rf_fuzz.ratio(a, b) / 100.0
except ImportError:
    def _levenshtein_ratio(a: str, b: str) -> float:
        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0
        n, m = len(a), len(b)
        d = [[0] * (m + 1) for _ in range(n + 1)]
        for i in range(n + 1):
            d[i][0] = i
        for j in range(m + 1):
            d[0][j] = j
        for i in range(1, n + 1):
            for j in range(1, m + 1):
                cost = 0 if a[i - 1] == b[j - 1] else 1
                d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost)
        return 1.0 - d[n][m] / max(n, m)


# ═══════════════════════════════════════════════════════════════════════════
# S3 — Name Similarity
# ═══════════════════════════════════════════════════════════════════════════

_STRIP_PREFIXES = [
    "afs ", "dts ", "dtc ", "premier subscription ", "reduced ",
]

_STRIP_TEMPORAL_RE = re.compile(
    r"\s*("
    r"vs?\s+ly[\s%-]*\d*"
    r"|vs?\s+lw[\s%-]*"
    r"|ly[\s-]*\d*"
    r"|lw"
    r"|htd|wtd|mtd|ytd"
    r")\s*$",
    re.IGNORECASE,
)

_STRIP_SYMBOLS_RE = re.compile(r"[%()]+")

# Ordered by longest-first so greedy replacement works correctly
_TRANSFORM_RULES: List[Tuple[str, str]] = [
    ("returns sales", "retail return"),
    ("return sales", "retail return"),
    ("book stock", "stock"),
    ("order count", "total orders"),
    ("page views", "views"),
    ("value cover", "buy value cover"),
    ("final destination", ""),
    ("first destination", ""),
    ("units", "quantity"),
    ("wac", "weighted average unit cost"),
]

_ACRONYM_MAP: Dict[str, str] = {
    "abv average basket value": "average basket value abv",
    "average selling price": "average selling price asp",
}

_TEMPORAL_TRANSFORMS: Dict[str, str] = {
    "vs ly %": "yoy %",
    "vs ly%": "yoy%",
    "vs lw %": "wow %",
    "vs lw%": "wow%",
    "% ly": "yoy %",
    "% lw": "wow %",
}

_NAME_STOPWORDS = {
    "the", "a", "an", "of", "for", "in", "by", "and", "or", "to", "is",
}


def normalize_name(name: str) -> str:
    """Normalize a metric/attribute name for comparison."""
    if not name:
        return ""
    s = name.lower().strip()
    for pfx in _STRIP_PREFIXES:
        if s.startswith(pfx):
            s = s[len(pfx):]
    s = _STRIP_SYMBOLS_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_temporal(name: str) -> str:
    """Remove temporal suffixes for base-name comparison."""
    s = name
    for _ in range(3):
        s2 = _STRIP_TEMPORAL_RE.sub("", s).strip()
        if s2 == s:
            break
        s = s2
    return s


def _apply_transforms(name: str) -> str:
    """Apply known MSTR->PBI naming transforms."""
    s = name
    for old, new in _TRANSFORM_RULES:
        s = s.replace(old, new)
    s = re.sub(r"\s+", " ", s).strip()
    if s in _ACRONYM_MAP:
        s = _ACRONYM_MAP[s]
    return s


def _jaccard_tokens(a: str, b: str) -> float:
    ta = set(a.split()) - _NAME_STOPWORDS
    tb = set(b.split()) - _NAME_STOPWORDS
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _extract_temporal_suffix(name: str) -> str:
    """Extract the temporal suffix from a name (LY, LW, HTD, etc.)."""
    patterns = [
        r"\s+(vs\s+ly[\s%-]*\d*)$",
        r"\s+(vs\s+lw[\s%-]*)$",
        r"\s+(ly[\s-]*\d*)$",
        r"\s+(lw)$",
        r"\s+(htd)$", r"\s+(wtd)$", r"\s+(mtd)$", r"\s+(ytd)$",
    ]
    for pat in patterns:
        m = re.search(pat, name, re.IGNORECASE)
        if m:
            return m.group(1).strip().lower()
    return ""


def name_similarity(mstr_name: str, pbi_name: str) -> float:
    """
    Compute a 0-1 similarity score between an MSTR name and a PBI name.
    Uses normalization, known transforms, Jaccard tokens, and Levenshtein.
    Penalizes temporal suffix mismatches (e.g. base metric matching "LY" variant).
    """
    mn = normalize_name(mstr_name)
    pn = normalize_name(pbi_name)

    if not mn or not pn:
        return 0.0

    # Check temporal suffix compatibility
    mn_temporal = _extract_temporal_suffix(mn)
    pn_temporal = _extract_temporal_suffix(pn)
    temporal_mismatch = (mn_temporal != pn_temporal)

    # Strip temporal for base comparison
    mn_base = _strip_temporal(mn)
    pn_base = _strip_temporal(pn)

    # 1. Exact match after normalization (including temporal)
    if mn == pn:
        return 1.0

    # 2. Exact base match
    if mn_base == pn_base:
        if temporal_mismatch:
            return 0.45  # penalized: same base but different temporal variant
        return 1.0

    # 3. Apply transform rules
    mn_transformed = _apply_transforms(mn_base)
    if mn_transformed == pn_base:
        if temporal_mismatch:
            return 0.42
        return 0.95

    pn_transformed = _apply_transforms(pn_base)
    if mn_base == pn_transformed:
        if temporal_mismatch:
            return 0.42
        return 0.95
    if mn_transformed == pn_transformed:
        if temporal_mismatch:
            return 0.40
        return 0.92

    # 4. Token Jaccard + Levenshtein
    jaccard = _jaccard_tokens(mn_transformed, pn_base)
    lev = _levenshtein_ratio(mn_transformed, pn_base)
    combined = 0.6 * jaccard + 0.4 * lev

    if temporal_mismatch and combined > 0.5:
        combined *= 0.6

    return round(combined, 4)


# ═══════════════════════════════════════════════════════════════════════════
# S1 — Direct Neo4j Mapping
# ═══════════════════════════════════════════════════════════════════════════

def signal_s1(mstr_obj: Dict) -> Optional[Dict[str, str]]:
    """
    Return the direct PBI mapping if present in Neo4j properties.
    Returns dict with pbi_name, pbi_model, confidence=1.0 or None.
    """
    pbi_name = mstr_obj.get("updated_pb_semantic_name") or mstr_obj.get("pb_semantic_name")
    pbi_model = mstr_obj.get("updated_pb_semantic_model") or mstr_obj.get("pb_semantic_model")

    if pbi_name and pbi_name.strip():
        return {
            "pbi_name": pbi_name.strip(),
            "pbi_model": (pbi_model or "").strip(),
            "confidence": 1.0,
            "signal": "S1",
        }
    return None


# ═══════════════════════════════════════════════════════════════════════════
# S2 — Column Lineage
# ═══════════════════════════════════════════════════════════════════════════

def signal_s2(
    mstr_obj: Dict,
    pbi_columns: List[Dict],
) -> Optional[Dict]:
    """
    Match MSTR ade_db_column to PBI sourceColumn on the same table.
    """
    ade_col = mstr_obj.get("ade_column") or ""
    ade_table = mstr_obj.get("ade_table") or ""
    if not ade_col:
        return None

    ade_col_norm = ade_col.lower().strip()
    best_score = 0.0
    best_match = None

    for pc in pbi_columns:
        src_col = (pc.get("sourceColumn") or "").lower().strip()
        if not src_col:
            continue

        if ade_col_norm == src_col:
            # Boost if table also matches
            table_match = False
            src_fqn = (pc.get("source_table_fqn") or "").lower()
            if ade_table and ade_table.lower() in src_fqn:
                table_match = True

            score = 0.95 if table_match else 0.75
            if score > best_score:
                best_score = score
                best_match = {
                    "pbi_name": pc["pbi_name"],
                    "pbi_model": pc.get("pbi_model", ""),
                    "pbi_table": pc.get("pbi_table", ""),
                    "confidence": score,
                    "signal": "S2",
                }

    return best_match


# ═══════════════════════════════════════════════════════════════════════════
# S3 — Name Similarity (batch matching)
# ═══════════════════════════════════════════════════════════════════════════

def signal_s3(
    mstr_obj: Dict,
    pbi_targets: List[Dict],
    top_k: int = 3,
) -> List[Dict]:
    """
    Find the top-K PBI targets by name similarity.
    pbi_targets: list of dicts with at least 'pbi_name', 'pbi_model', 'pbi_type'.
    """
    mstr_name = mstr_obj.get("name", "")
    mstr_type = mstr_obj.get("type", "")
    if not mstr_name:
        return []

    candidates = []
    for pt in pbi_targets:
        # Type gating: Metrics -> Measures, Attributes -> Columns
        pt_type = pt.get("pbi_type", "")
        if mstr_type == "Metric" and pt_type not in ("Measure", ""):
            continue
        if mstr_type == "Attribute" and pt_type not in ("Column", ""):
            continue

        score = name_similarity(mstr_name, pt["pbi_name"])
        if score >= 0.3:
            candidates.append({
                "pbi_name": pt["pbi_name"],
                "pbi_model": pt.get("pbi_model", ""),
                "pbi_type": pt_type,
                "confidence": score,
                "signal": "S3",
            })

    candidates.sort(key=lambda x: x["confidence"], reverse=True)
    return candidates[:top_k]


# ═══════════════════════════════════════════════════════════════════════════
# S4 — Formula / DAX Structure Analysis
# ═══════════════════════════════════════════════════════════════════════════

_MSTR_AGG_RE = re.compile(r"^(Sum|Count|Max|Min|RunningSum)\s*\(\s*(.+)\s*\)$", re.IGNORECASE)
_DAX_AGG_RE = re.compile(
    r"^(SUM|COUNT|DISTINCTCOUNT|MAX|MIN|AVERAGE)\s*\(\s*'([^']+)'\[([^\]]+)\]\s*\)$",
    re.IGNORECASE,
)
_DAX_MEASURE_REF_RE = re.compile(r"\[([^\]]+)\]")


def _parse_mstr_formula_type(formula: str) -> Dict:
    """Classify an MSTR formula into a structural type."""
    if not formula:
        return {"type": "unknown"}

    f = formula.strip()

    # Simple aggregation: Sum ( col )
    m = _MSTR_AGG_RE.match(f)
    if m:
        return {"type": "agg", "func": m.group(1).lower(), "column": m.group(2).strip()}

    # Binary expression: ( A op B )
    if f.startswith("(") and f.endswith(")"):
        inner = f[1:-1].strip()
        for op_char, op_name in [(" / ", "div"), (" - ", "sub"), (" + ", "add"), (" * ", "mul")]:
            # Find top-level operator (not inside nested parens)
            depth = 0
            for i, ch in enumerate(inner):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                elif depth == 0 and inner[i:i + len(op_char)] == op_char:
                    left = inner[:i].strip()
                    right = inner[i + len(op_char):].strip()
                    return {
                        "type": "expr",
                        "op": op_name,
                        "left": left,
                        "right": right,
                    }

    # IF expression
    if f.upper().startswith("IF"):
        return {"type": "conditional", "raw": f}

    # Metric reference
    return {"type": "ref", "name": f}


def _parse_dax_type(dax: str) -> Dict:
    """Classify a PBI DAX expression into a structural type."""
    if not dax:
        return {"type": "unknown"}

    d = dax.strip()

    # Simple aggregation: SUM ( 'Table'[Col] )
    m = _DAX_AGG_RE.match(d)
    if m:
        return {"type": "agg", "func": m.group(1).lower(), "table": m.group(2), "column": m.group(3)}

    # DIVIDE function
    if d.upper().startswith("DIVIDE"):
        return {"type": "expr", "op": "div", "raw": d}

    # Binary expression with measure refs: [A] - [B]
    refs = _DAX_MEASURE_REF_RE.findall(d)
    if refs:
        for op_char, op_name in [(" - ", "sub"), (" + ", "add"), (" / ", "div"), (" * ", "mul")]:
            if op_char in d:
                return {"type": "expr", "op": op_name, "refs": refs}
        return {"type": "ref", "refs": refs}

    return {"type": "unknown"}


_AGG_COMPAT = {
    ("sum", "sum"): 1.0,
    ("count", "count"): 1.0,
    ("count", "distinctcount"): 0.8,
    ("max", "max"): 1.0,
    ("min", "min"): 1.0,
}


def signal_s4(
    mstr_obj: Dict,
    pbi_measures: List[Dict],
    top_k: int = 3,
) -> List[Dict]:
    """
    Compare MSTR formula structure against PBI DAX expressions.
    Only applies to Metrics (Attributes have no formula).
    """
    formula = mstr_obj.get("formula", "")
    if not formula or mstr_obj.get("type") != "Metric":
        return []

    mstr_parsed = _parse_mstr_formula_type(formula)
    if mstr_parsed["type"] == "unknown":
        return []

    candidates = []

    for pm in pbi_measures:
        dax = pm.get("expression", "")
        if not dax:
            continue

        dax_parsed = _parse_dax_type(dax)
        if dax_parsed["type"] == "unknown":
            continue

        score = _compare_parsed(mstr_parsed, dax_parsed, mstr_obj.get("name", ""), pm["pbi_name"])
        if score >= 0.3:
            candidates.append({
                "pbi_name": pm["pbi_name"],
                "pbi_model": pm.get("pbi_model", ""),
                "confidence": round(score, 4),
                "signal": "S4",
            })

    candidates.sort(key=lambda x: x["confidence"], reverse=True)
    return candidates[:top_k]


def _compare_parsed(mstr_p: Dict, dax_p: Dict, mstr_name: str, pbi_name: str) -> float:
    """Compare two parsed formula structures."""
    # Both are simple aggregations
    if mstr_p["type"] == "agg" and dax_p["type"] == "agg":
        func_score = _AGG_COMPAT.get((mstr_p["func"], dax_p["func"]), 0.0)
        col_score = name_similarity(mstr_p["column"], dax_p["column"])
        return 0.4 * func_score + 0.6 * col_score

    # Both are binary expressions with same operator
    if mstr_p["type"] == "expr" and dax_p["type"] == "expr":
        if mstr_p.get("op") == dax_p.get("op"):
            # Use name similarity as proxy for operand matching
            ns = name_similarity(mstr_name, pbi_name)
            return 0.3 + 0.5 * ns
        return 0.1

    # Same structural type -> partial credit via name
    if mstr_p["type"] == dax_p["type"]:
        ns = name_similarity(mstr_name, pbi_name)
        return 0.3 * ns

    return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# S5 — Table Context Overlap
# ═══════════════════════════════════════════════════════════════════════════

def signal_s5(
    mstr_obj: Dict,
    pbi_table_sources: Dict[str, str],
) -> float:
    """
    Return bonus confidence (0-1) based on whether MSTR and PBI share
    a logical source table.
    pbi_table_sources: mapping of "pbi_model/pbi_table" -> source_table_fqn
    """
    mstr_tables = mstr_obj.get("lineage_source_tables") or []
    ade_table = mstr_obj.get("ade_table") or ""

    if not mstr_tables and not ade_table:
        return 0.0

    search_terms = set()
    if ade_table:
        search_terms.add(ade_table.lower().split(".")[-1])
    for t in mstr_tables:
        if isinstance(t, str):
            search_terms.add(t.lower().split(".")[-1])

    for key, src_fqn in pbi_table_sources.items():
        src_table = src_fqn.lower().split(".")[-1] if src_fqn else ""
        if src_table and src_table in search_terms:
            return 1.0

    return 0.0
