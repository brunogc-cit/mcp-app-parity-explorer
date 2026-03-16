/**
 * MCP App Server — Parity Mapping Explorer
 *
 * Connects to the existing flow-microstrategy-prd-http MCP server
 * as an upstream data source (READ-ONLY) and exposes interactive
 * parity analysis tools with UI resources.
 */

import {
  registerAppResource,
  registerAppTool,
  RESOURCE_MIME_TYPE,
} from "@modelcontextprotocol/ext-apps/server";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  handleSearchReports,
  handleRunParityAnalysis,
  handleGetMappingDetail,
  handleGetParitySummary,
} from "./src/tool-handlers.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DIST_DIR = path.join(__dirname, "dist");

export function createServer(workspaceRoot?: string): McpServer {
  const server = new McpServer({
    name: "Parity Mapping Explorer",
    version: "1.0.0",
  });

  const wsRoot = workspaceRoot || process.env.WORKSPACE_ROOT || path.resolve(__dirname, "../..");
  const resourceUri = "ui://parity-explorer/app.html";

  // ─── Tool 1: search-reports ───────────────────────────────────────────
  registerAppTool(
    server,
    "search-reports",
    {
      title: "Search Reports",
      description: "Search MicroStrategy reports by name or GUID. Returns a list of matching reports.",
      inputSchema: { query: z.string().describe("Report name (partial) or GUID") },
      _meta: { ui: { resourceUri } },
    },
    async ({ query }) => handleSearchReports({ query }),
  );

  // ─── Tool 2: run-parity-analysis ──────────────────────────────────────
  registerAppTool(
    server,
    "run-parity-analysis",
    {
      title: "Run Parity Analysis",
      description: "Run full 5-signal parity analysis for a report's metrics and attributes against Power BI models.",
      inputSchema: { guid: z.string().describe("Report GUID (32 hex chars)") },
      _meta: { ui: { resourceUri } },
    },
    async ({ guid }) => handleRunParityAnalysis({ guid }, wsRoot),
  );

  // ─── Tool 3: get-mapping-detail ───────────────────────────────────────
  registerAppTool(
    server,
    "get-mapping-detail",
    {
      title: "Get Mapping Detail",
      description: "Get detailed signal breakdown for a specific MSTR metric or attribute.",
      inputSchema: { guid: z.string().describe("MSTR object GUID") },
      _meta: { ui: { resourceUri } },
    },
    async ({ guid }) => handleGetMappingDetail({ guid }, wsRoot),
  );

  // ─── Tool 4: get-parity-summary ───────────────────────────────────────
  registerAppTool(
    server,
    "get-parity-summary",
    {
      title: "Get Parity Summary",
      description: "Get aggregate parity status and confidence statistics for a report.",
      inputSchema: { guid: z.string().describe("Report GUID") },
      _meta: { ui: { resourceUri } },
    },
    async ({ guid }) => handleGetParitySummary({ guid }),
  );

  // ─── UI Resource ──────────────────────────────────────────────────────
  registerAppResource(
    server,
    resourceUri,
    resourceUri,
    { mimeType: RESOURCE_MIME_TYPE },
    async () => {
      const html = await fs.readFile(path.join(DIST_DIR, "mcp-app.html"), "utf-8");
      return {
        contents: [{ uri: resourceUri, mimeType: RESOURCE_MIME_TYPE, text: html }],
      };
    },
  );

  return server;
}
