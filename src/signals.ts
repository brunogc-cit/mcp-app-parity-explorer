/**
 * Matching signals S1-S5 for MSTR-to-PBI mapping.
 * TypeScript port of parity-mapping/scripts/signals.py
 */

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

export interface MstrItem {
  guid: string;
  name: string;
  type: string; // "Metric" | "Attribute" | "DerivedMetric"
  formula?: string;
  parity_status?: string;
  pb_semantic_name?: string;
  updated_pb_semantic_name?: string;
  pb_semantic_model?: string;
  updated_pb_semantic_model?: string;
  ade_table?: string;
  ade_column?: string;
  edw_table?: string;
  edw_column?: string;
  priority?: string;
  lineage_source_tables?: string[];
}

export interface PbiTarget {
  pbi_name: string;
  pbi_model: string;
  pbi_type: string; // "Measure" | "Column"
  expression?: string;
  sourceColumn?: string;
  source_table_fqn?: string;
  pbi_table?: string;
}

export interface SignalResult {
  pbi_name: string;
  pbi_model: string;
  confidence: number;
  signal: string;
  pbi_type?: string;
  pbi_table?: string;
}

export interface MappingResult {
  mstr_guid: string;
  mstr_name: string;
  mstr_type: string;
  parity_status: string;
  best_match: SignalResult | null;
  final_score: number;
  confidence_level: string;
  signals: {
    s1: SignalResult | null;
    s2: SignalResult | null;
    s3: SignalResult[];
    s4: SignalResult[];
    s5: number;
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// Levenshtein
// ═══════════════════════════════════════════════════════════════════════════

function levenshteinRatio(a: string, b: string): number {
  if (!a && !b) return 1.0;
  if (!a || !b) return 0.0;
  const n = a.length;
  const m = b.length;
  const d: number[][] = Array.from({ length: n + 1 }, () => Array(m + 1).fill(0));
  for (let i = 0; i <= n; i++) d[i][0] = i;
  for (let j = 0; j <= m; j++) d[0][j] = j;
  for (let i = 1; i <= n; i++) {
    for (let j = 1; j <= m; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);
    }
  }
  return 1.0 - d[n][m] / Math.max(n, m);
}

// ═══════════════════════════════════════════════════════════════════════════
// S3 — Name Similarity
// ═══════════════════════════════════════════════════════════════════════════

const STRIP_PREFIXES = ["afs ", "dts ", "dtc ", "premier subscription ", "reduced "];

const STRIP_TEMPORAL_RE =
  /\s*(vs?\s+ly[\s%-]*\d*|vs?\s+lw[\s%-]*|ly[\s-]*\d*|lw|htd|wtd|mtd|ytd)\s*$/i;

const STRIP_SYMBOLS_RE = /[%()]+/g;

const TRANSFORM_RULES: [string, string][] = [
  ["returns sales", "retail return"],
  ["return sales", "retail return"],
  ["book stock", "stock"],
  ["order count", "total orders"],
  ["page views", "views"],
  ["value cover", "buy value cover"],
  ["final destination", ""],
  ["first destination", ""],
  ["units", "quantity"],
  ["wac", "weighted average unit cost"],
];

const ACRONYM_MAP: Record<string, string> = {
  "abv average basket value": "average basket value abv",
  "average selling price": "average selling price asp",
};

const NAME_STOPWORDS = new Set([
  "the", "a", "an", "of", "for", "in", "by", "and", "or", "to", "is",
]);

export function normalizeName(name: string): string {
  if (!name) return "";
  let s = name.toLowerCase().trim();
  for (const pfx of STRIP_PREFIXES) {
    if (s.startsWith(pfx)) s = s.slice(pfx.length);
  }
  s = s.replace(STRIP_SYMBOLS_RE, "");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}

function stripTemporal(name: string): string {
  let s = name;
  for (let i = 0; i < 3; i++) {
    const s2 = s.replace(STRIP_TEMPORAL_RE, "").trim();
    if (s2 === s) break;
    s = s2;
  }
  return s;
}

function applyTransforms(name: string): string {
  let s = name;
  for (const [old, rep] of TRANSFORM_RULES) {
    s = s.replace(old, rep);
  }
  s = s.replace(/\s+/g, " ").trim();
  if (s in ACRONYM_MAP) s = ACRONYM_MAP[s];
  return s;
}

function jaccardTokens(a: string, b: string): number {
  const ta = new Set(a.split(" ").filter((w) => !NAME_STOPWORDS.has(w)));
  const tb = new Set(b.split(" ").filter((w) => !NAME_STOPWORDS.has(w)));
  if (ta.size === 0 && tb.size === 0) return 1.0;
  if (ta.size === 0 || tb.size === 0) return 0.0;
  let intersection = 0;
  for (const t of ta) if (tb.has(t)) intersection++;
  const union = new Set([...ta, ...tb]).size;
  return intersection / union;
}

function extractTemporalSuffix(name: string): string {
  const patterns = [
    /\s+(vs\s+ly[\s%-]*\d*)$/i,
    /\s+(vs\s+lw[\s%-]*)$/i,
    /\s+(ly[\s-]*\d*)$/i,
    /\s+(lw)$/i,
    /\s+(htd)$/i,
    /\s+(wtd)$/i,
    /\s+(mtd)$/i,
    /\s+(ytd)$/i,
  ];
  for (const pat of patterns) {
    const m = name.match(pat);
    if (m) return m[1].trim().toLowerCase();
  }
  return "";
}

export function nameSimilarity(mstrName: string, pbiName: string): number {
  const mn = normalizeName(mstrName);
  const pn = normalizeName(pbiName);
  if (!mn || !pn) return 0.0;

  const mnTemporal = extractTemporalSuffix(mn);
  const pnTemporal = extractTemporalSuffix(pn);
  const temporalMismatch = mnTemporal !== pnTemporal;

  const mnBase = stripTemporal(mn);
  const pnBase = stripTemporal(pn);

  // 1. Exact match after normalization
  if (mn === pn) return 1.0;

  // 2. Exact base match
  if (mnBase === pnBase) {
    return temporalMismatch ? 0.45 : 1.0;
  }

  // 3. Apply transform rules
  const mnTransformed = applyTransforms(mnBase);
  if (mnTransformed === pnBase) return temporalMismatch ? 0.42 : 0.95;

  const pnTransformed = applyTransforms(pnBase);
  if (mnBase === pnTransformed) return temporalMismatch ? 0.42 : 0.95;
  if (mnTransformed === pnTransformed) return temporalMismatch ? 0.40 : 0.92;

  // 4. Token Jaccard + Levenshtein
  const jaccard = jaccardTokens(mnTransformed, pnBase);
  const lev = levenshteinRatio(mnTransformed, pnBase);
  let combined = 0.6 * jaccard + 0.4 * lev;

  if (temporalMismatch && combined > 0.5) combined *= 0.6;

  return Math.round(combined * 10000) / 10000;
}

// ═══════════════════════════════════════════════════════════════════════════
// S1 — Direct Neo4j Mapping
// ═══════════════════════════════════════════════════════════════════════════

export function signalS1(mstrObj: MstrItem): SignalResult | null {
  const pbiName =
    mstrObj.updated_pb_semantic_name || mstrObj.pb_semantic_name;
  const pbiModel =
    mstrObj.updated_pb_semantic_model || mstrObj.pb_semantic_model;

  if (pbiName && pbiName.trim()) {
    return {
      pbi_name: pbiName.trim(),
      pbi_model: (pbiModel || "").trim(),
      confidence: 1.0,
      signal: "S1",
    };
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// S2 — Column Lineage
// ═══════════════════════════════════════════════════════════════════════════

export function signalS2(
  mstrObj: MstrItem,
  pbiColumns: PbiTarget[],
): SignalResult | null {
  const adeCol = mstrObj.ade_column || "";
  const adeTable = mstrObj.ade_table || "";
  if (!adeCol) return null;

  const adeColNorm = adeCol.toLowerCase().trim();
  let bestScore = 0.0;
  let bestMatch: SignalResult | null = null;

  for (const pc of pbiColumns) {
    const srcCol = (pc.sourceColumn || "").toLowerCase().trim();
    if (!srcCol) continue;

    if (adeColNorm === srcCol) {
      let tableMatch = false;
      const srcFqn = (pc.source_table_fqn || "").toLowerCase();
      if (adeTable && srcFqn.includes(adeTable.toLowerCase())) {
        tableMatch = true;
      }

      const score = tableMatch ? 0.95 : 0.75;
      if (score > bestScore) {
        bestScore = score;
        bestMatch = {
          pbi_name: pc.pbi_name,
          pbi_model: pc.pbi_model || "",
          pbi_table: pc.pbi_table || "",
          confidence: score,
          signal: "S2",
        };
      }
    }
  }

  return bestMatch;
}

// ═══════════════════════════════════════════════════════════════════════════
// S3 — Name Similarity (batch matching)
// ═══════════════════════════════════════════════════════════════════════════

export function signalS3(
  mstrObj: MstrItem,
  pbiTargets: PbiTarget[],
  topK = 3,
): SignalResult[] {
  const mstrName = mstrObj.name || "";
  const mstrType = mstrObj.type || "";
  if (!mstrName) return [];

  const candidates: SignalResult[] = [];
  for (const pt of pbiTargets) {
    const ptType = pt.pbi_type || "";
    if (mstrType === "Metric" && ptType !== "Measure" && ptType !== "") continue;
    if (mstrType === "Attribute" && ptType !== "Column" && ptType !== "") continue;

    const score = nameSimilarity(mstrName, pt.pbi_name);
    if (score >= 0.3) {
      candidates.push({
        pbi_name: pt.pbi_name,
        pbi_model: pt.pbi_model || "",
        pbi_type: ptType,
        confidence: score,
        signal: "S3",
      });
    }
  }

  candidates.sort((a, b) => b.confidence - a.confidence);
  return candidates.slice(0, topK);
}

// ═══════════════════════════════════════════════════════════════════════════
// S4 — Formula / DAX Structure Analysis
// ═══════════════════════════════════════════════════════════════════════════

interface ParsedFormula {
  type: string;
  func?: string;
  column?: string;
  table?: string;
  op?: string;
  left?: string;
  right?: string;
  refs?: string[];
  raw?: string;
  name?: string;
}

const MSTR_AGG_RE = /^(Sum|Count|Max|Min|RunningSum)\s*\(\s*(.+)\s*\)$/i;
const DAX_AGG_RE =
  /^(SUM|COUNT|DISTINCTCOUNT|MAX|MIN|AVERAGE)\s*\(\s*'([^']+)'\[([^\]]+)\]\s*\)$/i;
const DAX_MEASURE_REF_RE = /\[([^\]]+)\]/g;

function parseMstrFormulaType(formula: string): ParsedFormula {
  if (!formula) return { type: "unknown" };
  const f = formula.trim();

  const m = MSTR_AGG_RE.exec(f);
  if (m) return { type: "agg", func: m[1].toLowerCase(), column: m[2].trim() };

  if (f.startsWith("(") && f.endsWith(")")) {
    const inner = f.slice(1, -1).trim();
    const ops: [string, string][] = [
      [" / ", "div"],
      [" - ", "sub"],
      [" + ", "add"],
      [" * ", "mul"],
    ];
    for (const [opChar, opName] of ops) {
      let depth = 0;
      for (let i = 0; i < inner.length; i++) {
        if (inner[i] === "(") depth++;
        else if (inner[i] === ")") depth--;
        else if (depth === 0 && inner.slice(i, i + opChar.length) === opChar) {
          return {
            type: "expr",
            op: opName,
            left: inner.slice(0, i).trim(),
            right: inner.slice(i + opChar.length).trim(),
          };
        }
      }
    }
  }

  if (f.toUpperCase().startsWith("IF")) return { type: "conditional", raw: f };
  return { type: "ref", name: f };
}

function parseDaxType(dax: string): ParsedFormula {
  if (!dax) return { type: "unknown" };
  const d = dax.trim();

  const m = DAX_AGG_RE.exec(d);
  if (m) return { type: "agg", func: m[1].toLowerCase(), table: m[2], column: m[3] };

  if (d.toUpperCase().startsWith("DIVIDE")) return { type: "expr", op: "div", raw: d };

  const refs: string[] = [];
  let match;
  const re = new RegExp(DAX_MEASURE_REF_RE.source, "g");
  while ((match = re.exec(d)) !== null) refs.push(match[1]);

  if (refs.length > 0) {
    const ops: [string, string][] = [
      [" - ", "sub"],
      [" + ", "add"],
      [" / ", "div"],
      [" * ", "mul"],
    ];
    for (const [opChar, opName] of ops) {
      if (d.includes(opChar)) return { type: "expr", op: opName, refs };
    }
    return { type: "ref", refs };
  }

  return { type: "unknown" };
}

const AGG_COMPAT: Record<string, number> = {
  "sum,sum": 1.0,
  "count,count": 1.0,
  "count,distinctcount": 0.8,
  "max,max": 1.0,
  "min,min": 1.0,
};

function compareParsed(
  mstrP: ParsedFormula,
  daxP: ParsedFormula,
  mstrName: string,
  pbiName: string,
): number {
  if (mstrP.type === "agg" && daxP.type === "agg") {
    const funcScore = AGG_COMPAT[`${mstrP.func},${daxP.func}`] ?? 0.0;
    const colScore = nameSimilarity(mstrP.column || "", daxP.column || "");
    return 0.4 * funcScore + 0.6 * colScore;
  }

  if (mstrP.type === "expr" && daxP.type === "expr") {
    if (mstrP.op === daxP.op) {
      const ns = nameSimilarity(mstrName, pbiName);
      return 0.3 + 0.5 * ns;
    }
    return 0.1;
  }

  if (mstrP.type === daxP.type) {
    return 0.3 * nameSimilarity(mstrName, pbiName);
  }

  return 0.0;
}

export function signalS4(
  mstrObj: MstrItem,
  pbiMeasures: PbiTarget[],
  topK = 3,
): SignalResult[] {
  const formula = mstrObj.formula || "";
  if (!formula || mstrObj.type !== "Metric") return [];

  const mstrParsed = parseMstrFormulaType(formula);
  if (mstrParsed.type === "unknown") return [];

  const candidates: SignalResult[] = [];
  for (const pm of pbiMeasures) {
    const dax = pm.expression || "";
    if (!dax) continue;

    const daxParsed = parseDaxType(dax);
    if (daxParsed.type === "unknown") continue;

    const score = compareParsed(mstrParsed, daxParsed, mstrObj.name || "", pm.pbi_name);
    if (score >= 0.3) {
      candidates.push({
        pbi_name: pm.pbi_name,
        pbi_model: pm.pbi_model || "",
        confidence: Math.round(score * 10000) / 10000,
        signal: "S4",
      });
    }
  }

  candidates.sort((a, b) => b.confidence - a.confidence);
  return candidates.slice(0, topK);
}

// ═══════════════════════════════════════════════════════════════════════════
// S5 — Table Context Overlap
// ═══════════════════════════════════════════════════════════════════════════

export function signalS5(
  mstrObj: MstrItem,
  pbiTableSources: Record<string, string>,
): number {
  const mstrTables = mstrObj.lineage_source_tables || [];
  const adeTable = mstrObj.ade_table || "";

  if (mstrTables.length === 0 && !adeTable) return 0.0;

  const searchTerms = new Set<string>();
  if (adeTable) searchTerms.add(adeTable.toLowerCase().split(".").pop()!);
  for (const t of mstrTables) {
    if (typeof t === "string") searchTerms.add(t.toLowerCase().split(".").pop()!);
  }

  for (const srcFqn of Object.values(pbiTableSources)) {
    const srcTable = srcFqn ? srcFqn.toLowerCase().split(".").pop()! : "";
    if (srcTable && searchTerms.has(srcTable)) return 1.0;
  }

  return 0.0;
}

// ═══════════════════════════════════════════════════════════════════════════
// Scoring & Classification
// ═══════════════════════════════════════════════════════════════════════════

const WEIGHTS = { s2: 0.3, s3: 0.35, s4: 0.25, s5: 0.1 };
const THRESHOLDS = {
  confirmed: 0.9,
  high: 0.7,
  medium: 0.5,
  low: 0.3,
};

export function classify(score: number): string {
  if (score >= THRESHOLDS.confirmed) return "Confirmed";
  if (score >= THRESHOLDS.high) return "High";
  if (score >= THRESHOLDS.medium) return "Medium";
  if (score >= THRESHOLDS.low) return "Low";
  return "Unmapped";
}

export function computeMapping(
  mstrItem: MstrItem,
  pbiTargets: PbiTarget[],
  pbiTableSources: Record<string, string>,
): MappingResult {
  // S1 — authoritative direct mapping
  const s1 = signalS1(mstrItem);
  if (s1) {
    return {
      mstr_guid: mstrItem.guid,
      mstr_name: mstrItem.name,
      mstr_type: mstrItem.type,
      parity_status: mstrItem.parity_status || "Unknown",
      best_match: s1,
      final_score: 1.0,
      confidence_level: "Confirmed",
      signals: { s1, s2: null, s3: [], s4: [], s5: 0 },
    };
  }

  // Filter PBI targets by type
  const pbiColumns = pbiTargets.filter((t) => t.pbi_type === "Column");
  const pbiMeasures = pbiTargets.filter((t) => t.pbi_type === "Measure");

  const s2 = signalS2(mstrItem, pbiColumns);
  const s3 = signalS3(mstrItem, pbiTargets);
  const s4 = signalS4(mstrItem, pbiMeasures);
  const s5 = signalS5(mstrItem, pbiTableSources);

  // Collect all unique PBI candidates
  const candidateMap = new Map<string, { s2: number; s3: number; s4: number; s5: number; result: SignalResult }>();

  const addCandidate = (r: SignalResult, signal: "s2" | "s3" | "s4") => {
    const key = `${r.pbi_model}/${r.pbi_name}`;
    if (!candidateMap.has(key)) {
      candidateMap.set(key, { s2: 0, s3: 0, s4: 0, s5, result: r });
    }
    candidateMap.get(key)![signal] = Math.max(candidateMap.get(key)![signal], r.confidence);
  };

  if (s2) addCandidate(s2, "s2");
  for (const c of s3) addCandidate(c, "s3");
  for (const c of s4) addCandidate(c, "s4");

  let bestScore = 0;
  let bestMatch: SignalResult | null = null;

  for (const [, c] of candidateMap) {
    const weighted =
      WEIGHTS.s2 * c.s2 +
      WEIGHTS.s3 * c.s3 +
      WEIGHTS.s4 * c.s4 +
      WEIGHTS.s5 * c.s5;

    const bestIndividual = Math.max(c.s2, c.s3, c.s4);

    let activeCount = 0;
    if (c.s2 >= 0.3) activeCount++;
    if (c.s3 >= 0.3) activeCount++;
    if (c.s4 >= 0.3) activeCount++;
    const multiBonus = activeCount > 1 ? 0.1 * (activeCount - 1) : 0;

    let final = Math.max(weighted, 0.6 * bestIndividual) + multiBonus;
    final = Math.min(final, 0.99);

    if (final > bestScore) {
      bestScore = final;
      bestMatch = { ...c.result, confidence: Math.round(final * 10000) / 10000 };
    }
  }

  return {
    mstr_guid: mstrItem.guid,
    mstr_name: mstrItem.name,
    mstr_type: mstrItem.type,
    parity_status: mstrItem.parity_status || "Unknown",
    best_match: bestMatch,
    final_score: Math.round(bestScore * 10000) / 10000,
    confidence_level: classify(bestScore),
    signals: { s1: null, s2, s3, s4, s5 },
  };
}
