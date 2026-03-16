/**
 * Extracted tool handlers — shared between MCP App server and Chat API.
 * All Neo4j operations are READ-ONLY.
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import fs from "node:fs/promises";
import path from "node:path";
import { computeMapping, type MstrItem, type PbiTarget, type MappingResult } from "./signals.js";

// ═══════════════════════════════════════════════════════════════════════════
// Upstream MCP Client (READ-ONLY)
// ═══════════════════════════════════════════════════════════════════════════

const UPSTREAM_URL = "https://ca-mcp-asos-prod.bravesand-dac45dfd.uksouth.azurecontainerapps.io/mcp";
const UPSTREAM_TOKEN = "XmSMmZax5wh9q1dg-DBMot3aeYublCSmLg4sYFEJhIs";
const WRITE_OPS_RE = /\b(CREATE|DELETE|SET|REMOVE|MERGE|DROP|DETACH)\b/i;

let upstreamClient: Client | null = null;

async function getUpstreamClient(): Promise<Client> {
  if (upstreamClient) return upstreamClient;
  const client = new Client({ name: "parity-explorer", version: "1.0.0" });
  const transport = new StreamableHTTPClientTransport(
    new URL(UPSTREAM_URL),
    { requestInit: { headers: { Authorization: `Bearer ${UPSTREAM_TOKEN}` } } },
  );
  await client.connect(transport);
  upstreamClient = client;
  return client;
}

export async function callReadCypher(query: string): Promise<unknown> {
  if (WRITE_OPS_RE.test(query)) {
    throw new Error("READ-ONLY: Write operations are forbidden");
  }
  const client = await getUpstreamClient();
  return client.callTool({ name: "read-cypher", arguments: { query } });
}

// ═══════════════════════════════════════════════════════════════════════════
// PBI Extractor
// ═══════════════════════════════════════════════════════════════════════════

export interface PbiIndex {
  targets: PbiTarget[];
  tableSources: Record<string, string>;
}

let pbiIndexCache: PbiIndex | null = null;

export async function getPbiIndex(workspaceRoot: string): Promise<PbiIndex> {
  if (pbiIndexCache) return pbiIndexCache;
  pbiIndexCache = await scanPbiModels(workspaceRoot);
  return pbiIndexCache;
}

async function scanPbiModels(workspaceRoot: string): Promise<PbiIndex> {
  const targets: PbiTarget[] = [];
  const tableSources: Record<string, string> = {};
  const pbiRoot = path.join(workspaceRoot, "asos-data-ade-powerbi", "powerbi", "models");

  let modelDirs: string[];
  try {
    modelDirs = await fs.readdir(pbiRoot);
  } catch {
    console.warn("PBI models directory not found, using empty index");
    return { targets, tableSources };
  }

  for (const modelName of modelDirs) {
    const dbFile = path.join(pbiRoot, modelName, "database.json");
    try {
      const raw = await fs.readFile(dbFile, "utf-8");
      const db = JSON.parse(raw);
      const model = db?.model;
      if (!model?.tables) continue;

      for (const table of model.tables) {
        const tableName = table.name || "";
        if (table.partitions?.[0]?.source?.expression) {
          const expr = Array.isArray(table.partitions[0].source.expression)
            ? table.partitions[0].source.expression.join("\n")
            : table.partitions[0].source.expression;
          const fqnMatch = expr.match(/\[(\w+)\]\.\[(\w+)\]\.\[(\w+)\]/);
          if (fqnMatch) {
            tableSources[`${modelName}/${tableName}`] = `${fqnMatch[1]}.${fqnMatch[2]}.${fqnMatch[3]}`;
          }
        }
        if (table.measures) {
          for (const measure of table.measures) {
            targets.push({
              pbi_name: measure.name || "",
              pbi_model: modelName,
              pbi_type: "Measure",
              expression: Array.isArray(measure.expression) ? measure.expression.join("\n") : measure.expression || "",
              pbi_table: tableName,
            });
          }
        }
        if (table.columns) {
          for (const col of table.columns) {
            targets.push({
              pbi_name: col.name || "",
              pbi_model: modelName,
              pbi_type: "Column",
              sourceColumn: col.sourceColumn || "",
              source_table_fqn: tableSources[`${modelName}/${tableName}`] || "",
              pbi_table: tableName,
            });
          }
        }
      }
    } catch {
      // Skip unreadable model files
    }
  }
  return { targets, tableSources };
}

// ═══════════════════════════════════════════════════════════════════════════
// Neo4j result parser
// ═══════════════════════════════════════════════════════════════════════════

interface Neo4jRow { [key: string]: unknown; }

export function parseNeo4jResult(result: unknown): Neo4jRow[] {
  const content = (result as { content?: Array<{ type: string; text?: string }> })?.content;
  if (!content) return [];
  for (const item of content) {
    if (item.type === "text" && item.text) {
      try {
        const parsed = JSON.parse(item.text);
        if (parsed?.results?.[0]) {
          const r = parsed.results[0];
          const columns: string[] = r.columns || [];
          return (r.data || []).map((d: { row: unknown[] }) => {
            const obj: Neo4jRow = {};
            columns.forEach((col, i) => { obj[col] = d.row[i]; });
            return obj;
          });
        }
        if (Array.isArray(parsed)) return parsed;
        return [parsed];
      } catch { /* Not JSON */ }
    }
  }
  return [];
}

// ═══════════════════════════════════════════════════════════════════════════
// Helper: convert Neo4j row to MstrItem
// ═══════════════════════════════════════════════════════════════════════════

function rowToMstrItem(row: Neo4jRow): MstrItem {
  return {
    guid: String(row.guid || ""),
    name: String(row.name || ""),
    type: ((row.types as string[]) || []).find((t) =>
      ["Metric", "Attribute", "DerivedMetric"].includes(t),
    ) || "Metric",
    formula: String(row.formula || ""),
    parity_status: String(row.parity_status || ""),
    pb_semantic_name: String(row.pb_semantic_name || ""),
    pb_semantic_model: String(row.pb_model || ""),
    ade_table: String(row.ade_table || ""),
    ade_column: String(row.ade_column || ""),
    edw_table: String(row.edw_table || ""),
    edw_column: String(row.edw_column || ""),
    priority: String(row.priority || ""),
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// Tool Result type
// ═══════════════════════════════════════════════════════════════════════════

export interface ToolResult {
  [key: string]: unknown;
  content: Array<{ type: "text"; text: string }>;
}

// ═══════════════════════════════════════════════════════════════════════════
// Tool Handlers
// ═══════════════════════════════════════════════════════════════════════════

function escCypher(s: string): string {
  return s.replace(/'/g, "\\'");
}

export async function handleSearchReports(args: { query: string }): Promise<ToolResult> {
  const cypher = `
    MATCH (r:Report)
    WHERE toLower(r.name) CONTAINS toLower('${escCypher(args.query)}')
       OR r.guid CONTAINS '${escCypher(args.query)}'
    RETURN r.guid AS guid, r.name AS name, r.subtype AS subtype, r.location AS location
    LIMIT 20
  `;
  const result = await callReadCypher(cypher);
  const rows = parseNeo4jResult(result);
  return { content: [{ type: "text", text: JSON.stringify({ action: "search-results", data: rows }) }] };
}

export async function handleRunParityAnalysis(args: { guid: string }, wsRoot: string): Promise<ToolResult> {
  const cypher = `
    MATCH (r:Report {guid: '${escCypher(args.guid)}'})
    CALL {
      WITH r
      MATCH (r)-[:DEPENDS_ON]->(ma)
      WHERE ma:Metric OR ma:Attribute OR ma:DerivedMetric
      RETURN ma, 'direct' AS path
      UNION
      WITH r
      MATCH (r)-[:DEPENDS_ON]->(mid)-[:DEPENDS_ON]->(ma)
      WHERE (mid:Filter OR mid:Prompt OR mid:DerivedMetric)
        AND (ma:Metric OR ma:Attribute OR ma:DerivedMetric)
      RETURN ma, 'indirect' AS path
    }
    WITH DISTINCT ma,
         ma.guid AS guid, ma.name AS name, LABELS(ma) AS types,
         ma.formula AS formula,
         COALESCE(ma.updated_parity_status, ma.parity_status) AS parity_status,
         COALESCE(ma.updated_pb_semantic_name, ma.pb_semantic_name) AS pb_semantic_name,
         COALESCE(ma.updated_pb_semantic_model, ma.pb_semantic_model) AS pb_model,
         ma.ade_db_table AS ade_table, ma.ade_db_column AS ade_column,
         ma.edw_table AS edw_table, ma.edw_column AS edw_column,
         ma.inherited_priority_level AS priority
    RETURN guid, name, types, formula, parity_status, pb_semantic_name, pb_model,
           ade_table, ade_column, edw_table, edw_column, priority
  `;
  const result = await callReadCypher(cypher);
  const rows = parseNeo4jResult(result);
  const pbiIndex = await getPbiIndex(wsRoot);

  const mstrItems = rows.map(rowToMstrItem);
  const mappings: MappingResult[] = mstrItems.map((item) =>
    computeMapping(item, pbiIndex.targets, pbiIndex.tableSources),
  );

  const summary = {
    total: mappings.length,
    confirmed: mappings.filter((m) => m.confidence_level === "Confirmed").length,
    high: mappings.filter((m) => m.confidence_level === "High").length,
    medium: mappings.filter((m) => m.confidence_level === "Medium").length,
    low: mappings.filter((m) => m.confidence_level === "Low").length,
    unmapped: mappings.filter((m) => m.confidence_level === "Unmapped").length,
    parity: {
      Complete: mappings.filter((m) => m.parity_status === "Complete").length,
      Planned: mappings.filter((m) => m.parity_status === "Planned").length,
      Drop: mappings.filter((m) => m.parity_status === "Drop").length,
      "Not Planned": mappings.filter((m) => m.parity_status === "Not Planned").length,
      Unknown: mappings.filter((m) => !["Complete", "Planned", "Drop", "Not Planned"].includes(m.parity_status)).length,
    },
    coverage: mappings.length > 0
      ? Math.round(((mappings.length - mappings.filter((m) => m.confidence_level === "Unmapped").length) / mappings.length) * 100)
      : 0,
  };

  return {
    content: [{
      type: "text",
      text: JSON.stringify({
        action: "parity-analysis",
        report_guid: args.guid,
        summary,
        mappings: mappings.map((m) => ({
          guid: m.mstr_guid, name: m.mstr_name, type: m.mstr_type,
          parity_status: m.parity_status, score: m.final_score, level: m.confidence_level,
          match: m.best_match ? { name: m.best_match.pbi_name, model: m.best_match.pbi_model } : null,
          signals: {
            s1: m.signals.s1 ? m.signals.s1.confidence : 0,
            s2: m.signals.s2 ? m.signals.s2.confidence : 0,
            s3: m.signals.s3.length > 0 ? m.signals.s3[0].confidence : 0,
            s4: m.signals.s4.length > 0 ? m.signals.s4[0].confidence : 0,
            s5: m.signals.s5,
          },
        })),
      }),
    }],
  };
}

export async function handleGetMappingDetail(args: { guid: string }, wsRoot: string): Promise<ToolResult> {
  const cypher = `
    MATCH (ma)
    WHERE ma.guid = '${escCypher(args.guid)}'
      AND (ma:Metric OR ma:Attribute OR ma:DerivedMetric)
    RETURN ma.guid AS guid, ma.name AS name, LABELS(ma) AS types,
           ma.formula AS formula,
           COALESCE(ma.updated_parity_status, ma.parity_status) AS parity_status,
           COALESCE(ma.updated_pb_semantic_name, ma.pb_semantic_name) AS pb_semantic_name,
           COALESCE(ma.updated_pb_semantic_model, ma.pb_semantic_model) AS pb_model,
           ma.ade_db_table AS ade_table, ma.ade_db_column AS ade_column,
           ma.edw_table AS edw_table, ma.edw_column AS edw_column,
           ma.inherited_priority_level AS priority
  `;
  const result = await callReadCypher(cypher);
  const rows = parseNeo4jResult(result);

  if (rows.length === 0) {
    return { content: [{ type: "text", text: JSON.stringify({ action: "detail", error: "Not found" }) }] };
  }

  const pbiIndex = await getPbiIndex(wsRoot);
  const mstrItem = rowToMstrItem(rows[0]);
  const mapping = computeMapping(mstrItem, pbiIndex.targets, pbiIndex.tableSources);

  return {
    content: [{
      type: "text",
      text: JSON.stringify({
        action: "detail",
        item: {
          guid: mstrItem.guid, name: mstrItem.name, type: mstrItem.type,
          formula: mstrItem.formula, parity_status: mstrItem.parity_status,
          ade_table: mstrItem.ade_table, ade_column: mstrItem.ade_column, priority: mstrItem.priority,
        },
        mapping: {
          score: mapping.final_score, level: mapping.confidence_level, match: mapping.best_match,
          signals: { s1: mapping.signals.s1, s2: mapping.signals.s2, s3: mapping.signals.s3, s4: mapping.signals.s4, s5: mapping.signals.s5 },
        },
      }),
    }],
  };
}

export async function handleGetParitySummary(args: { guid: string }): Promise<ToolResult> {
  const cypher = `
    MATCH (r:Report {guid: '${escCypher(args.guid)}'})
    RETURN r.guid AS guid, r.name AS name, r.subtype AS subtype, r.location AS location
  `;
  const result = await callReadCypher(cypher);
  const reportRows = parseNeo4jResult(result);

  const parityCypher = `
    MATCH (r:Report {guid: '${escCypher(args.guid)}'})-[:DEPENDS_ON*1..2]->(ma)
    WHERE ma:Metric OR ma:Attribute OR ma:DerivedMetric
    WITH DISTINCT ma,
         COALESCE(ma.updated_parity_status, ma.parity_status, 'Unknown') AS status
    RETURN status, COUNT(*) AS count
    ORDER BY count DESC
  `;
  const parityResult = await callReadCypher(parityCypher);
  const parityRows = parseNeo4jResult(parityResult);

  return {
    content: [{
      type: "text",
      text: JSON.stringify({ action: "summary", report: reportRows[0] || null, parity: parityRows }),
    }],
  };
}

/** Dispatch a tool call by name */
export async function executeTool(name: string, args: Record<string, unknown>, wsRoot: string): Promise<ToolResult> {
  switch (name) {
    case "search-reports":
      return handleSearchReports(args as { query: string });
    case "run-parity-analysis":
      return handleRunParityAnalysis(args as { guid: string }, wsRoot);
    case "get-mapping-detail":
      return handleGetMappingDetail(args as { guid: string }, wsRoot);
    case "get-parity-summary":
      return handleGetParitySummary(args as { guid: string });
    default:
      return { content: [{ type: "text", text: JSON.stringify({ error: `Unknown tool: ${name}` }) }] };
  }
}
