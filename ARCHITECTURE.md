# Architecture — MCP App Parity Explorer

Technical deep-dive into every layer, file, data flow, and decision behind this application.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Layers of the Application](#2-layers-of-the-application)
3. [File-by-File Reference](#3-file-by-file-reference)
4. [Data Flow: End-to-End](#4-data-flow-end-to-end)
5. [The 5-Signal Matching Algorithm](#5-the-5-signal-matching-algorithm)
6. [Transport Modes: stdio vs HTTP](#6-transport-modes-stdio-vs-http)
7. [Chat Interface: How the Claude CLI Bridge Works](#7-chat-interface-how-the-claude-cli-bridge-works)
8. [MCP App UI: How the Iframe Rendering Works](#8-mcp-app-ui-how-the-iframe-rendering-works)
9. [Build System](#9-build-system)
10. [Package Dependencies](#10-package-dependencies)
11. [Security Model](#11-security-model)
12. [Key Design Decisions](#12-key-design-decisions)

---

## 1. Overview

This application is built as an **MCP App** — a Model Context Protocol server that exposes both tools (functions an LLM can call) and UI resources (HTML rendered inside sandboxed iframes by MCP-compatible clients).

It exists to solve one problem: **visualising how well MicroStrategy (MSTR) objects map to Power BI (PBI) equivalents** during a migration project. The analysis uses a 5-signal algorithm that combines Neo4j graph data, PBI model definitions, name matching, formula analysis, and lineage tracing.

The application has **three distinct interfaces**, all served from the same Express server:

```
┌────────────────────────────────────────────────────────────────────┐
│                     Express Server (port 3001)                     │
│                                                                    │
│  Interface 1: Chat (/chat.html)                                    │
│    Human types natural language → backend spawns claude CLI →       │
│    Claude calls MCP tools → results stream back as SSE →           │
│    tool results render as embedded iframes                         │
│                                                                    │
│  Interface 2: MCP App (/mcp-app.html)                              │
│    Rendered inside Claude Desktop / ChatGPT / VS Code Copilot →    │
│    communicates via MCP ext-apps protocol (postMessage) →          │
│    user clicks buttons → app calls server tools → renders charts   │
│                                                                    │
│  Interface 3: MCP Protocol (/mcp)                                  │
│    Raw MCP protocol endpoint (HTTP transport) →                    │
│    used by basic-host, other MCP clients →                         │
│    also available via stdio (--stdio flag)                         │
└────────────────────────────────────────────────────────────────────┘
```

---

## 2. Layers of the Application

The application is organized in 5 horizontal layers:

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 5: PRESENTATION (Browser)                                │
│  chat.html + src/chat.ts + src/chat-styles.css                  │
│  mcp-app.html + src/mcp-app.ts + src/styles.css                 │
│  Canvas charts (doughnut, radar), tables, filters, SSE reader   │
├─────────────────────────────────────────────────────────────────┤
│  Layer 4: API / TRANSPORT                                       │
│  main.ts — Express server, routes, static files, SSE            │
│  src/chat-api.ts — Claude CLI bridge, SSE streaming             │
│  MCP protocol: StreamableHTTPServerTransport + StdioTransport   │
├─────────────────────────────────────────────────────────────────┤
│  Layer 3: MCP SERVER                                            │
│  server.ts — MCP tool registration, UI resource registration    │
│  Uses @modelcontextprotocol/sdk + @modelcontextprotocol/ext-apps│
├─────────────────────────────────────────────────────────────────┤
│  Layer 2: BUSINESS LOGIC                                        │
│  src/tool-handlers.ts — Tool implementations, Neo4j queries     │
│  src/signals.ts — 5-signal matching algorithm (S1-S5)           │
│  src/tool-defs.ts — UI tool registry                            │
├─────────────────────────────────────────────────────────────────┤
│  Layer 1: DATA SOURCES (External)                               │
│  Neo4j (via upstream MCP server) — MSTR graph data              │
│  PBI model files (local filesystem) — database.json definitions │
└─────────────────────────────────────────────────────────────────┘
```

### Layer 1: Data Sources

**Neo4j** — The MSTR migration graph database stores reports, metrics, attributes, their relationships (`DEPENDS_ON`, `BELONGS_TO`), parity statuses, and manual PBI mappings. This application does NOT connect to Neo4j directly. Instead, it connects to an upstream MCP server (`flow-microstrategy-prd-http`) that provides a `read-cypher` tool for executing Cypher queries. This is a deliberate security boundary — we only have READ-ONLY access through the upstream server, and additionally enforce our own write-operation blocklist.

**PBI Models** — Power BI semantic model definitions live in the local filesystem as `database.json` files. These contain table definitions, measures (with DAX expressions), and columns (with source mappings). The tool-handlers scan these on first request and cache the index in memory.

### Layer 2: Business Logic

This is the core analytical engine. Three files:

- **`signals.ts`** — The 5-signal matching algorithm, ported from Python. Takes an MSTR item and a PBI index, returns a scored mapping result with signal breakdowns.
- **`tool-handlers.ts`** — Implements the 4 MCP tools. Composes Neo4j queries, parses results, runs signal matching, and returns structured JSON.
- **`tool-defs.ts`** — A simple registry of tool names that produce UI-renderable results (used by the chat frontend to decide which results get iframes).

### Layer 3: MCP Server

**`server.ts`** uses the MCP SDK to register 4 tools and 1 UI resource. Each tool uses `registerAppTool` (from `@modelcontextprotocol/ext-apps`) instead of the basic `server.tool()` — this tells MCP clients that these tools have an associated UI.

The UI resource is the compiled `mcp-app.html` — a self-contained HTML file that gets served as an iframe by MCP-compatible clients (Claude Desktop, ChatGPT, etc.).

### Layer 4: API / Transport

**`main.ts`** is the Express server that wires everything together. It supports two transport modes:
- **HTTP** (default): Express serves static files, chat API routes, and the MCP protocol endpoint at `/mcp`.
- **stdio** (`--stdio` flag): For Claude Desktop and other clients that communicate via stdin/stdout.

**`chat-api.ts`** implements the Claude CLI bridge — it spawns `claude --print --output-format stream-json` as a child process, parses the stream-json events, and forwards them as SSE to the browser.

### Layer 5: Presentation

Two independent frontend applications, both compiled by Vite into self-contained HTML files:

- **Chat** (`chat.html` + `src/chat.ts` + `src/chat-styles.css`) — A conversational interface where humans type natural language. The backend handles all LLM interaction.
- **MCP App** (`mcp-app.html` + `src/mcp-app.ts` + `src/styles.css`) — An interactive dashboard with search, charts, tables, and detail panels. Runs inside iframes in both the chat interface and MCP clients.

---

## 3. File-by-File Reference

### `main.ts` — Application Entry Point

**Purpose:** Bootstrap the Express server, register routes, and start listening.

**Key responsibilities:**
1. Determines transport mode: HTTP (default) or stdio (`--stdio` flag)
2. Creates the Express app with middleware in the correct order:
   - `express.static("dist")` — serves compiled HTML files. **Must** come before `express.json()` because Express 5's JSON parser can interfere with static file GET requests.
   - `express.json()` — parses JSON bodies for API and MCP routes.
3. Registers the chat API routes via `registerChatRoutes(app)`
4. Sets up the MCP protocol endpoint at `/mcp` — creates a fresh MCP server instance per request (stateless HTTP transport with no session persistence)
5. Root redirect: `GET /` → `/chat.html`
6. Graceful shutdown on SIGINT/SIGTERM

**Architecture note:** The MCP endpoint creates a new `McpServer` instance for every HTTP request. This is the stateless pattern recommended by the MCP SDK for HTTP transports — each request gets its own server/transport pair, with cleanup on response close.

```typescript
app.all("/mcp", async (req, res) => {
  const server = factory();           // Fresh server per request
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  res.on("close", () => {             // Cleanup when client disconnects
    transport.close();
    server.close();
  });
  await server.connect(transport);
  await transport.handleRequest(req, res, req.body);
});
```

---

### `server.ts` — MCP Server Definition

**Purpose:** Defines the MCP server with 4 tools and 1 UI resource.

**Key concepts:**

`registerAppTool` vs `server.tool`:
- `server.tool()` (from the MCP SDK) registers a basic tool that returns text/content.
- `registerAppTool()` (from `@modelcontextprotocol/ext-apps`) additionally declares that the tool has an associated UI resource via `_meta: { ui: { resourceUri } }`. When a compatible client (Claude Desktop, ChatGPT) sees this metadata, it renders the UI resource (an iframe) alongside the tool result.

All 4 tools point to the same UI resource (`ui://parity-explorer/app.html`) — the `mcp-app.html` file. The frontend code inside the iframe uses the `action` field in the tool result JSON to determine which view to render (search results, dashboard, detail, summary).

The `workspaceRoot` parameter is used for PBI model scanning. It defaults to `process.env.WORKSPACE_ROOT` or two levels up from the server file.

---

### `src/tool-handlers.ts` — Tool Implementations

**Purpose:** Contains all tool logic, shared between the MCP server (Layer 3) and the Chat API (Layer 4).

**Components:**

**Upstream MCP Client:**
```
tool-handlers.ts → MCP Client → HTTP → upstream MCP server → Neo4j
```
The upstream client is a singleton (`upstreamClient`) that connects via `StreamableHTTPClientTransport` to a remote Azure Container App hosting the `flow-microstrategy-prd-http` MCP server. Authentication is via Bearer token. The connection is lazy — initialized on first tool call.

**`callReadCypher(query)`:** Validates the Cypher query against a regex blocklist (CREATE, DELETE, SET, REMOVE, MERGE, DROP, DETACH), then delegates to the upstream `read-cypher` tool.

**`parseNeo4jResult(result)`:** Parses the upstream MCP tool response. The upstream returns Neo4j transactional API format:
```json
{
  "results": [{
    "columns": ["guid", "name"],
    "data": [{ "row": ["ABC123", "Sales Value"] }]
  }]
}
```
This gets transformed into an array of plain objects: `[{ guid: "ABC123", name: "Sales Value" }]`.

**PBI Index Scanner (`scanPbiModels`):** Reads all `database.json` files from `{workspace}/asos-data-ade-powerbi/powerbi/models/*/database.json`. For each model, extracts:
- **Measures**: name, model, DAX expression, table
- **Columns**: name, model, sourceColumn, source table FQN

The FQN (fully qualified name) is extracted from partition source expressions using regex: `[schema].[catalog].[table]`. This is used for S2 (lineage) and S5 (table context) signals.

The PBI index is cached in memory after first scan (`pbiIndexCache`).

**4 Tool Handlers:**

| Handler | Cypher Pattern | Uses PBI Index? | Output Action |
|---------|---------------|-----------------|---------------|
| `handleSearchReports` | `MATCH (r:Report) WHERE name CONTAINS query` | No | `search-results` |
| `handleRunParityAnalysis` | `MATCH (r:Report)-[:DEPENDS_ON*1..2]->(ma)` | Yes — runs 5-signal matching | `parity-analysis` |
| `handleGetMappingDetail` | `MATCH (ma) WHERE guid = ?` | Yes — single item matching | `detail` |
| `handleGetParitySummary` | Two queries: report info + parity counts | No | `summary` |

**`executeTool(name, args, wsRoot)`:** A dispatcher function that routes by tool name. Used by the Chat API's `POST /api/tool` endpoint for iframe follow-up requests.

---

### `src/signals.ts` — The 5-Signal Matching Algorithm

**Purpose:** TypeScript port of the Python analysis tools. Implements the complete MSTR-to-PBI matching algorithm.

See [Section 5](#5-the-5-signal-matching-algorithm) for detailed algorithm documentation.

**Exports:**
- `signalS1(mstrObj)` → `SignalResult | null` — Direct Neo4j mapping check
- `signalS2(mstrObj, pbiColumns)` → `SignalResult | null` — Column lineage matching
- `signalS3(mstrObj, pbiTargets, topK)` → `SignalResult[]` — Name similarity ranking
- `signalS4(mstrObj, pbiMeasures, topK)` → `SignalResult[]` — Formula structure comparison
- `signalS5(mstrObj, pbiTableSources)` → `number` — Table context overlap (binary)
- `computeMapping(mstrItem, pbiTargets, pbiTableSources)` → `MappingResult` — Full scoring pipeline
- `normalizeName(name)` — Name normalization (used internally and available for testing)
- `nameSimilarity(mstrName, pbiName)` — Pairwise name comparison
- `classify(score)` — Score → confidence level classification

---

### `src/tool-defs.ts` — UI Tool Registry

**Purpose:** A simple `Set` of tool names that produce UI-renderable results.

```typescript
export const UI_TOOLS = new Set([
  "search-reports",
  "run-parity-analysis",
  "get-mapping-detail",
  "get-parity-summary",
]);
```

Used by the chat frontend (`chat.ts`) to decide which tool results should be rendered as iframes vs plain text.

---

### `src/chat-api.ts` — Claude CLI Bridge

**Purpose:** Express routes that bridge between the browser chat interface and the local `claude` CLI.

See [Section 7](#7-chat-interface-how-the-claude-cli-bridge-works) for detailed flow documentation.

**Routes:**

| Route | Method | Purpose |
|-------|--------|---------|
| `/api/chat` | POST | SSE streaming — spawns claude, parses stream-json, forwards events |
| `/api/tool` | POST | Direct tool execution — for iframe follow-up requests |

**Key implementation details:**

**Environment sanitization:** Before spawning the claude process, ALL environment variables starting with `CLAUDE` are deleted. This prevents "nested session" detection when running inside Claude Code (VS Code extension), which sets `CLAUDECODE=1`, `CLAUDE_AGENT_SDK_VERSION`, `CLAUDE_CODE_ENTRYPOINT`, etc.

**CWD selection:** The claude CLI is spawned with `cwd` set to the `asos-agentic-workflow/` directory, which contains the `.mcp.json` file. This is how Claude discovers the parity-explorer MCP tools — the `.mcp.json` in CWD is automatically loaded.

**Express 5 SSE compatibility:** The `res.on("close")` handler (not `req.on("close")`) is used for client disconnect detection. In Express 5, `req.on("close")` fires prematurely, which would kill the claude process before it produces output.

**stream-json parsing:** The claude CLI outputs one JSON object per line. The readline interface parses each line and routes by `event.type`:
- `system` → `SSE event: system`
- `assistant` → extracts text deltas and tool_use blocks → `SSE events: delta, tool_call`
- `user` → extracts tool_result content → `SSE event: tool_result`
- `result` → extracts cost/duration → `SSE event: done`

**Text delta computation:** The claude stream-json format sends the FULL accumulated text on each assistant event (not incremental deltas). The code tracks `lastAssistantText` and computes the delta: `newText.slice(lastAssistantText.length)`.

---

### `chat.html` — Chat Interface Entry Point

**Purpose:** HTML shell for the chat interface. Minimal markup — all logic is in `src/chat.ts`.

**Structure:**
- Header: title ("Parity Explorer"), subtitle, status dot + text
- Messages area: scrollable container for chat bubbles
- Welcome message with 3 suggestion buttons
- Input bar: auto-resizing textarea + send button (SVG arrow icon)

---

### `src/chat.ts` — Chat Client-Side Logic

**Purpose:** Manages the chat conversation state, SSE stream reading, and dynamic DOM updates.

**State:**
```typescript
const messages: Array<{ role: string; content: string }> = [];  // Conversation history
let isStreaming = false;  // Prevents concurrent requests
```

**`sendMessage()` flow:**
1. Takes text from textarea, adds to `messages` array
2. Creates a user bubble (right-aligned, blue)
3. Creates an assistant bubble (left-aligned, dark) with typing indicator
4. POST to `/api/chat` with full `messages` array
5. Reads response body as a stream via `response.body.getReader()`
6. Parses SSE lines from the stream buffer
7. Routes each SSE data payload to `handleSSEData()`

**SSE Event Handling:**

| SSE Event | Browser Action |
|-----------|---------------|
| `delta` (has `text`) | Appends text to assistant bubble, renders basic markdown |
| `tool_call` (has `name`, `id`, `input`) | Shows tool notice ("Calling search-reports...") with gear icon |
| `tool_result` (has `data`, `hasUi: true`) | Creates iframe with `src="/mcp-app.html"`, injects data via postMessage |
| `done` (has `cost`, `duration`) | Shows metadata footer (e.g., "4.2s · $0.0876") |
| `error` (has `message`) | Shows error bubble |

**Iframe communication:**
- When a tool result arrives, a new iframe is created with `src="/mcp-app.html"`.
- After the iframe loads (500ms delay for initialization), data is injected via `postMessage({ type: "inject-tool-result", data })`.
- The chat listens for `message` events from iframes. If an iframe sends `{ type: "tool-request", name, arguments }`, the chat proxies it to `POST /api/tool` and sends the result back to the iframe.

**Markdown rendering:** Basic inline rendering: `**bold**`, `*italic*`, `` `code` ``, and newlines → `<br>`. No block-level elements (code blocks, headers, lists).

---

### `src/chat-styles.css` — Chat UI Styles

**Purpose:** Complete dark theme for the chat interface, matching the MCP App's visual language.

**Design system (CSS custom properties):**

| Variable | Value | Usage |
|----------|-------|-------|
| `--bg-primary` | `#0f1117` | Page background |
| `--bg-secondary` | `#1a1d2e` | Header, input bar |
| `--bg-tertiary` | `#232738` | Assistant bubbles |
| `--bg-input` | `#2a2e42` | Input field background |
| `--accent` | `#6c8aff` | Links, active states, send button |
| `--user-bubble` | `#2563eb` | User message background |
| `--success` | `#22c55e` | Ready status dot |
| `--warning` | `#f59e0b` | Thinking/tool status dot |
| `--error` | `#ef4444` | Error states |

**Status dot animations:** The status dot in the header pulses at different rates:
- `ready` — solid green, no animation
- `thinking` — amber, 1s pulse
- `streaming` — blue, 0.5s pulse (fast)
- `tool` — amber, 0.7s pulse

**Layout:** Flexbox column with `height: 100vh`. Messages area is `flex: 1` with `overflow-y: auto`. Max width 900px, centered.

---

### `mcp-app.html` — MCP App Entry Point

**Purpose:** HTML shell for the interactive dashboard UI. Contains all the structural markup for the dashboard views.

**Views (all initially hidden, shown by JavaScript):**
- Search bar + results dropdown
- Loading spinner
- Dashboard: KPI bar (5 metrics) + charts (2 canvas) + filter bar + data table + detail panel
- Empty state

---

### `src/mcp-app.ts` — MCP App Client-Side Logic

**Purpose:** The interactive dashboard application that runs inside iframes. Handles both MCP host communication and chat iframe injection.

**MCP App Connection:**
```typescript
const app = new App({ name: "Parity Mapping Explorer", version: "1.0.0" });
app.ontoolresult = (result) => { /* handle LLM tool results */ };
app.connect();
```

The `App` class (from `@modelcontextprotocol/ext-apps`) communicates with the MCP host (Claude Desktop, ChatGPT, etc.) via postMessage. When the LLM calls a tool and the host renders the UI, `app.ontoolresult` fires with the result data.

**Dual-mode operation:** The app detects if it's running inside a chat iframe (`window.parent !== window`) and overrides `app.callServerTool`:
1. Try the MCP host first (2-second timeout)
2. If timeout/error, fall back to asking the parent chat window via postMessage
3. Parent chat proxies the request to `/api/tool`

This means the same `mcp-app.html` works in both contexts:
- **In Claude Desktop:** Uses MCP protocol natively
- **In chat iframe:** Uses postMessage → HTTP → tool handler

**Charts:** Custom Canvas 2D implementations (no external library):
- **Doughnut chart** (`drawDoughnut`): Renders confidence distribution and parity status. Supports inner radius (hollow center), legend, and center text.
- **Radar chart** (`drawRadar`): Shows 5-signal breakdown for a selected item. Pentagon grid with axes, filled data polygon, and data points.

**Data handler (`handleServerData`):** Routes by `action` field:
- `search-results` → renders clickable report list
- `parity-analysis` → renders full dashboard (KPIs, charts, table)
- `detail` → renders signal detail view
- `summary` → renders summary view

**Table features:**
- Sortable columns (click header to toggle asc/desc)
- Filter chips by confidence level (All/Confirmed/High/Medium/Low/Unmapped)
- Filter chips by type (All/Metrics/Attributes)
- Score bar visualization per row
- Signal dots (5 circles, filled when signal fired, S1 in green)
- Click row → detail panel with radar chart

**Model context updates:** When the user clicks a table row, the app calls `app.updateModelContext()` to tell the MCP host what the user selected. This lets the LLM know which item the user is looking at, enabling conversational follow-ups like "tell me more about this metric".

---

### `src/styles.css` — MCP App Styles

**Purpose:** Complete dark theme for the MCP App dashboard.

**Design system:** Uses the same color palette as the chat interface but with slightly different variable names:

| Variable | Semantic Meaning |
|----------|-----------------|
| `--confirmed` / `#22c55e` | Confirmed confidence, Complete parity |
| `--high` / `#3b82f6` | High confidence |
| `--medium` / `#f59e0b` | Medium confidence, Planned parity |
| `--low` / `#ef4444` | Low confidence, Drop parity |
| `--unmapped` / `#6b7280` | Unmapped, Not Planned parity |

These colors are consistent across charts, badges, filter chips, score bars, and signal dots.

---

### `vite.config.ts` — Build Configuration

**Purpose:** Configures Vite to build self-contained HTML files.

**Key features:**
- **`vite-plugin-singlefile`**: Inlines all JavaScript and CSS into the HTML file. This is required for MCP App compatibility — the HTML must be fully self-contained because MCP hosts serve it from a `data:` URI or blob.
- **`INPUT` env var**: Selects which HTML file to build (`mcp-app.html` or `chat.html`). The build script runs Vite twice, once for each file.
- **`emptyOutDir: false`**: Prevents the second build from deleting the first build's output.
- **Development mode**: Inline sourcemaps, no minification.

---

### `tsconfig.json` — Type Checking Config

**Purpose:** Used by `tsc --noEmit` for type checking only (no output files).

- Target: ESNext
- Module resolution: `bundler` (compatible with Vite)
- Includes: `src/`, `server.ts`, `main.ts`
- Strict mode with `noUnusedLocals` and `noUnusedParameters`

---

### `tsconfig.server.json` — Server Declaration Config

**Purpose:** Emits `.d.ts` type declaration files for `server.ts` and `main.ts`.

- Target: ES2022
- Module resolution: `NodeNext` (for Node.js runtime compatibility)
- Output: `dist/` directory (declaration files only)

---

### `skills/parity-mapping/` — Skill Definition

**Purpose:** Contains the prompt and tools for Claude agents to use the parity analysis system. This is NOT used by the MCP App itself — it's for the broader ASOS agentic workflow system.

| File | Purpose |
|------|---------|
| `SKILL.md` | Full prompt with instructions, signal docs, execution patterns |
| `scripts/config.py` | Python config: repo discovery, signal weights, thresholds |
| `scripts/signals.py` | Python implementation of the 5-signal algorithm |
| `scripts/run_mapping.py` | Full dataset analysis CLI |
| `scripts/run_lit_report.py` | Single report analysis |
| `scripts/run_batch_reports.py` | Multi-report HTML dashboard |
| `scripts/extract_mstr.py` | MSTR cache extraction from Neo4j |
| `scripts/extract_pbi.py` | PBI model extraction |
| `scripts/extract_dbt.py` | dbt lineage extraction |
| `scripts/requirements.txt` | Python dependencies |
| `references/neo4j-schema.md` | Graph database schema reference |
| `references/signal-tuning-guide.md` | Weight and threshold tuning guide |
| `references/sample-output.md` | Example report formats |
| `evals/evals.json` | Evaluation test configurations |

---

## 4. Data Flow: End-to-End

### Flow A: Chat Interface → Tool Execution → Iframe Rendering

```
                    BROWSER                          SERVER                        EXTERNAL
                    -------                          ------                        --------

User types:         ┌──────────┐
"Search Monday      │ chat.ts  │
 Huddle"            │ sendMsg()│
                    └────┬─────┘
                         │ POST /api/chat
                         │ Body: { messages: [{ role:"user", content:"Search..." }] }
                         │
                         ▼
                    ┌──────────┐
                    │ chat-api │ SSE headers → flushHeaders()
                    │ .ts      │
                    └────┬─────┘
                         │ spawn claude --print --output-format stream-json
                         │ --system-prompt "..." "Search Monday Huddle"
                         │ CWD: asos-agentic-workflow/ (has .mcp.json)
                         │
                         ▼
                    ┌──────────┐
                    │ claude   │ Reads .mcp.json → discovers parity-explorer
                    │ CLI      │ Connects to parity-explorer MCP server (stdio)
                    │          │ Calls search-reports({ query: "Monday Huddle" })
                    └────┬─────┘
                         │ MCP tool call → server.ts → tool-handlers.ts
                         │
                         ▼
                    ┌──────────┐                    ┌─────────┐
                    │ tool-    │ callReadCypher() → │ upstream │
                    │ handlers │ MATCH (r:Report)   │ MCP      │→ Neo4j
                    │ .ts      │ WHERE name CONTAINS│ server   │
                    └────┬─────┘ 'Monday Huddle'    └─────────┘
                         │
                         │ Returns: { action: "search-results", data: [...] }
                         │
                         ▼
                    ┌──────────┐
                    │ claude   │ stream-json output:
                    │ CLI      │ {"type":"assistant","message":{"content":[
                    │          │   {"type":"tool_use","name":"search-reports",...},
                    │          │   {"type":"text","text":"I found 3 reports..."}
                    │          │ ]}}
                    │          │ {"type":"user","message":{"content":[
                    │          │   {"type":"tool_result","content":[...]}
                    │          │ ]}}
                    └────┬─────┘
                         │
                         ▼
                    ┌──────────┐
                    │ chat-api │ Parses stream-json → sends SSE events:
                    │ .ts      │   event: tool_call → { name: "search-reports" }
                    │          │   event: tool_result → { data: {...}, hasUi: true }
                    │          │   event: delta → { text: "I found 3 reports..." }
                    │          │   event: done → { cost: 0.08, duration: 4200 }
                    └────┬─────┘
                         │ SSE stream
                         ▼
                    ┌──────────┐
                    │ chat.ts  │ handleSSEData():
                    │          │   tool_call → shows "Calling search-reports..."
                    │          │   tool_result → creates <iframe src="/mcp-app.html">
                    │          │   delta → appends text to assistant bubble
                    │          │   done → shows "4.2s · $0.0876"
                    └────┬─────┘
                         │
                         │ iframe.onload (500ms delay)
                         │ postMessage({ type: "inject-tool-result", data })
                         │
                         ▼
                    ┌──────────┐
                    │ mcp-app  │ handleServerData({ action: "search-results", data: [...] })
                    │ .ts      │ renderSearchResults() → clickable cards
                    │ (iframe) │
                    └──────────┘
```

### Flow B: User Clicks Report in Iframe → Analysis

```
User clicks report     ┌──────────┐
in iframe              │ mcp-app  │ analyzeReport(guid)
                       │ .ts      │ app.callServerTool({ name: "run-parity-analysis" })
                       │ (iframe) │
                       └────┬─────┘
                            │ MCP host available? → try first (2s timeout)
                            │ No? → postMessage to parent:
                            │   { type: "tool-request", name: "run-parity-analysis", arguments: { guid } }
                            │
                            ▼
                       ┌──────────┐
                       │ chat.ts  │ window.addEventListener("message"):
                       │ (parent) │   receives tool-request
                       └────┬─────┘
                            │ POST /api/tool
                            │ Body: { name: "run-parity-analysis", arguments: { guid } }
                            │
                            ▼
                       ┌──────────┐                    ┌─────────┐
                       │ chat-api │ executeTool() →     │ tool-   │
                       │ .ts      │ handleRunParity() → │ handlers│
                       │          │                     │ .ts     │
                       └────┬─────┘                    └────┬────┘
                            │                               │
                            │    callReadCypher()    ←──────┘
                            │    getPbiIndex()       ←──────┘
                            │    computeMapping()    ←──────┘
                            │
                            ▼
                       ┌──────────┐
                       │ chat.ts  │ Receives JSON response
                       │ (parent) │ postMessage to iframe:
                       │          │   { type: "inject-tool-result", data: { action: "parity-analysis", ... } }
                       └────┬─────┘
                            │
                            ▼
                       ┌──────────┐
                       │ mcp-app  │ handleServerData({ action: "parity-analysis", ... })
                       │ .ts      │ renderDashboard() → KPIs, doughnut charts, data table
                       │ (iframe) │
                       └──────────┘
```

### Flow C: MCP Protocol (Claude Desktop / basic-host)

```
Claude Desktop    ┌──────────┐
or basic-host     │ MCP      │ stdio or HTTP transport
                  │ Client   │
                  └────┬─────┘
                       │ JSON-RPC 2.0:
                       │ { "method": "tools/call", "params": { "name": "search-reports", "arguments": { "query": "..." } } }
                       │
                       ▼
                  ┌──────────┐
                  │ server.ts│ registerAppTool callback:
                  │          │ async ({ query }) => handleSearchReports({ query })
                  └────┬─────┘
                       │
                       ▼ (same as Flow A from here — tool-handlers.ts → Neo4j + PBI)
                       │
                       │ Returns JSON-RPC result with:
                       │ 1. Tool content (text with JSON)
                       │ 2. UI resource reference (ui://parity-explorer/app.html)
                       │
                       ▼
                  ┌──────────┐
                  │ MCP      │ Client requests the UI resource
                  │ Client   │ → server returns dist/mcp-app.html (self-contained HTML)
                  └────┬─────┘ → client renders in sandboxed iframe
                       │       → data injected via ext-apps postMessage protocol
                       ▼
                  ┌──────────┐
                  │ mcp-app  │ Same app, same code — but communicating via
                  │ .ts      │ App class (ext-apps protocol) instead of parent postMessage
                  │ (iframe) │
                  └──────────┘
```

---

## 5. The 5-Signal Matching Algorithm

The algorithm in `src/signals.ts` determines how confidently an MSTR object (metric or attribute) maps to a PBI equivalent.

### Signal S1 — Direct Neo4j Mapping (Authoritative)

**Source:** `pb_semantic_name` / `updated_pb_semantic_name` property on the MSTR node in Neo4j.

**Logic:** If the property exists, a human has manually verified this mapping. Return immediately with score 1.0 ("Confirmed"). No other signals are evaluated.

**Code path:** `signalS1(mstrObj)` → checks `updated_pb_semantic_name || pb_semantic_name` → returns `{ confidence: 1.0, signal: "S1" }` or null.

### Signal S2 — Column Lineage (weight: 0.30)

**Source:** MSTR `ade_db_column` property → traced through PBI column definitions.

**Logic:** For each PBI column, compare `ade_db_column` (MSTR) with `sourceColumn` (PBI). If they match:
- Table also matches (via `source_table_fqn`): score = 0.95
- Column match only: score = 0.75

Returns the best-scoring match.

**Code path:** `signalS2(mstrObj, pbiColumns)` → iterates all PBI columns → case-insensitive comparison → returns best match.

### Signal S3 — Name Similarity (weight: 0.35)

**Source:** MSTR `name` vs PBI `name`.

**The most complex signal.** The `nameSimilarity()` function applies multiple normalization and comparison stages:

1. **Normalization** (`normalizeName`):
   - Lowercase
   - Strip known prefixes: "afs ", "dts ", "dtc ", "premier subscription ", "reduced "
   - Remove symbols: `%`, `(`, `)`
   - Collapse whitespace

2. **Temporal suffix handling** (`stripTemporal`, `extractTemporalSuffix`):
   - Detect suffixes: LY, LW, HTD, WTD, MTD, YTD, "vs LY", "vs LW"
   - Strip from both names for base comparison
   - If temporal suffixes differ between MSTR and PBI, apply 0.6x penalty

3. **Comparison cascade** (short-circuits on match):
   - Exact match after normalization → 1.0
   - Exact base match (temporal stripped) → 1.0 (or 0.45 if temporal mismatch)
   - Transform rules applied (domain synonyms) → 0.95 (or 0.42)
   - Both sides transformed → 0.92 (or 0.40)
   - Token Jaccard + Levenshtein → `0.6 * jaccard + 0.4 * levenshtein`

4. **Domain transforms** (`applyTransforms`):
   ```
   "returns sales" → "retail return"
   "book stock"    → "stock"
   "order count"   → "total orders"
   "page views"    → "views"
   "units"         → "quantity"
   ```

5. **Acronym expansion:**
   ```
   "abv average basket value" → "average basket value abv"
   ```

6. **Stopword removal** for Jaccard: "the", "a", "an", "of", "for", "in", "by", "and", "or", "to", "is"

**Type filtering:** S3 respects type compatibility:
- MSTR Metric → only matches PBI Measure
- MSTR Attribute → only matches PBI Column

Returns top K candidates (default 3) sorted by score.

### Signal S4 — Formula Analysis (weight: 0.25, metrics only)

**Source:** MSTR `formula` vs PBI DAX `expression`.

**Both formulas are parsed into a structural type:**

| Type | MSTR Pattern | DAX Pattern |
|------|-------------|-------------|
| `agg` | `Sum(Column)`, `Count(Col)` | `SUM('Table'[Column])` |
| `expr` | `(A / B)`, `(A - B)` | `DIVIDE(...)`, `[A] - [B]` |
| `conditional` | `IF(...)` | (not parsed further) |
| `ref` | plain name reference | `[Measure]` references |

**Comparison rules:**
- `agg` vs `agg`: 40% function compatibility (SUM↔SUM=1.0, COUNT↔DISTINCTCOUNT=0.8) + 60% column name similarity
- `expr` vs `expr`: Same operator? → 0.3 + 0.5 × name similarity. Different operator? → 0.1
- Same type → 0.3 × name similarity
- Different types → 0.0

Returns top K candidates (default 3) sorted by score.

### Signal S5 — Table Context Overlap (weight: 0.10)

**Source:** MSTR `lineage_source_tables` / `ade_table` vs PBI partition source tables.

**Logic:** Binary signal (0.0 or 1.0). If any MSTR source table name (last segment after `.`) matches any PBI partition source table name, return 1.0.

This signal never produces a match on its own — it reinforces other signals via the multi-signal bonus.

### Scoring Pipeline (`computeMapping`)

```
1. Check S1 → if found, return immediately: score=1.0, level="Confirmed"

2. Compute S2, S3, S4, S5 independently

3. Collect all unique PBI candidates across signals
   (keyed by "model/name")

4. For each candidate, compute final score:
   a. Weighted average: S2×0.30 + S3×0.35 + S4×0.25 + S5×0.10
   b. Best individual: max(S2, S3, S4)
   c. Multi-signal bonus: +0.10 per extra active signal (>=2 signals with score >=0.30)
   d. Final = max(weighted, 0.6 × best_individual) + bonus
   e. Cap at 0.99 (only S1 can reach 1.0)

5. Select candidate with highest final score

6. Classify: >=0.90 Confirmed | >=0.70 High | >=0.50 Medium | >=0.30 Low | <0.30 Unmapped
```

The `max(weighted, 0.6 × best_individual)` formula means that a single strong signal can drive the score, even if other signals are absent. The 0.6 factor prevents a single signal from dominating entirely.

---

## 6. Transport Modes: stdio vs HTTP

The MCP protocol supports two transport mechanisms. This application supports both:

### stdio (for Claude Desktop, Cursor)

```bash
npx tsx main.ts --stdio
```

- Uses `StdioServerTransport` from the MCP SDK
- Reads JSON-RPC from stdin, writes to stdout
- Single session, long-lived connection
- Claude Desktop and Cursor spawn this as a child process

### HTTP (default, for browser + basic-host)

```bash
npx tsx main.ts
```

- Uses `StreamableHTTPServerTransport` from the MCP SDK
- Express route at `/mcp` handles HTTP requests
- Stateless: new server instance per request (`sessionIdGenerator: undefined`)
- Supports both the Chat interface and direct MCP protocol access

---

## 7. Chat Interface: How the Claude CLI Bridge Works

The chat interface is a "mini Claude Desktop" that runs in the browser. Instead of calling the Anthropic API directly, it spawns the locally installed `claude` CLI.

### Why use the CLI instead of the API?

1. **No API key required** — uses the user's existing Claude Code authentication
2. **Automatic MCP discovery** — the CLI reads `.mcp.json` from its CWD and connects to MCP servers
3. **Full agentic loop** — the CLI handles multi-turn tool calling internally (tool call → execute → resume with result → repeat)
4. **Consistent behavior** — same model, system prompt, and tool capabilities as Claude Code

### The SSE Protocol

```
Browser                     Express                      claude CLI
  |                            |                            |
  |-- POST /api/chat --------->|                            |
  |   { messages: [...] }      |                            |
  |                            |-- spawn ------------------>|
  |                            |   --print                  |
  |                            |   --output-format stream-json
  |                            |   --verbose                |
  |                            |   --dangerously-skip-permissions
  |                            |   --system-prompt "..."    |
  |                            |   "Search Monday Huddle"   |
  |                            |                            |
  |<-- SSE: system ------------|<-- stream-json: system ----|
  |    { subtype: "init" }     |   { type: "system" }      |
  |                            |                            |
  |<-- SSE: tool_call ---------|<-- stream-json: assistant -|
  |    { name, id, input }     |   { tool_use block }      |
  |                            |                            |
  |                            |   (claude executes tool    |
  |                            |    via MCP internally)     |
  |                            |                            |
  |<-- SSE: tool_result -------|<-- stream-json: user ------|
  |    { data, hasUi: true }   |   { tool_result block }   |
  |                            |                            |
  |<-- SSE: delta -------------|<-- stream-json: assistant -|
  |    { text: "I found..." }  |   { text block }          |
  |                            |                            |
  |<-- SSE: done --------------|<-- stream-json: result ----|
  |    { cost, duration }      |   { total_cost_usd }      |
  |                            |                            |
  |   (connection closes)      |   (process exits)         |
```

### Conversation context

The `claude --print` command is stateless — each invocation is a new session. To maintain conversation context, the chat API formats the full message history into a single prompt:

```
Previous conversation:
User: Search for Monday Huddle report
Assistant: I found 3 reports matching "Monday Huddle"...

Current question:
Run parity analysis on the first one
```

This means every message in the conversation costs input tokens for the full history.

---

## 8. MCP App UI: How the Iframe Rendering Works

The MCP App (`mcp-app.html`) operates in two modes depending on its context:

### Mode 1: MCP Host (Claude Desktop, ChatGPT)

```
MCP Host
├── renders mcp-app.html in sandboxed iframe
├── communicates via postMessage (ext-apps protocol)
│   ├── host → iframe: tool result data (app.ontoolresult)
│   └── iframe → host: tool calls (app.callServerTool)
│       and model context updates (app.updateModelContext)
```

The `App` class from `@modelcontextprotocol/ext-apps` handles all communication. The host pushes tool results when the LLM calls a tool, and the app can request additional tool calls via `callServerTool`.

### Mode 2: Chat Iframe (this application's chat interface)

```
Chat window (parent)
├── creates <iframe src="/mcp-app.html">
├── injects data via postMessage:
│   { type: "inject-tool-result", data: { action: "search-results", ... } }
├── listens for tool requests from iframe:
│   { type: "tool-request", name: "run-parity-analysis", arguments: { guid } }
├── proxies to POST /api/tool
└── sends result back to iframe:
    { type: "inject-tool-result", data: { action: "parity-analysis", ... } }
```

The `mcp-app.ts` code detects it's in an iframe (`window.parent !== window`) and overrides `app.callServerTool` to:
1. Try the MCP host first (2-second timeout via `Promise.race`)
2. Fall back to postMessage to the parent chat window

This dual-mode approach means the same HTML file works everywhere without modification.

### Iframe sandbox

Chat iframes are created with: `sandbox="allow-scripts allow-same-origin"`. This allows JavaScript execution and same-origin postMessage, but blocks:
- Form submission
- Navigation
- Popups
- Plugins
- Top-level navigation

---

## 9. Build System

### Vite Build (Frontend)

Two separate Vite builds produce two self-contained HTML files:

```bash
# Build 1: MCP App
cross-env INPUT=mcp-app.html vite build

# Build 2: Chat
cross-env INPUT=chat.html vite build
```

The `INPUT` environment variable selects the entry point. `vite-plugin-singlefile` inlines all JS and CSS into the HTML.

**Why single-file?** MCP hosts serve the app HTML as a self-contained resource. External script/CSS references would 404 because the iframe's origin is different from the server. Single-file eliminates this problem.

**Output sizes:**
- `dist/mcp-app.html` — ~136KB (includes all chart code, table rendering, signal algorithm)
- `dist/chat.html` — ~12KB (SSE reader, DOM manipulation, basic markdown)

### TypeScript Build (Server)

```bash
# Type check only (no output)
tsc --noEmit

# Emit declaration files for server
tsc -p tsconfig.server.json
```

The server code runs via `tsx` (TypeScript eXecute) — no compilation step needed for execution. The declaration emission is for IDE support.

### Development Mode

```bash
npm start
# Runs concurrently:
# 1. vite build --watch (mcp-app.html)
# 2. vite build --watch (chat.html)
# 3. tsx watch main.ts (auto-restart server on changes)
```

---

## 10. Package Dependencies

### Runtime Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `@modelcontextprotocol/sdk` | ^1.27.1 | MCP server/client SDK — protocol implementation, transports |
| `@modelcontextprotocol/ext-apps` | ^1.2.2 | MCP App extensions — `registerAppTool`, `registerAppResource`, `App` class |
| `express` | ^5.2.1 | HTTP server — routes, middleware, static files |
| `cors` | ^2.8.6 | CORS middleware — allows cross-origin requests to /mcp endpoint |

### Dev Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `typescript` | ^5.9.3 | Type checking |
| `tsx` | ^4.21.0 | TypeScript execution for Node.js (no compilation step) |
| `vite` | ^8.0.0 | Frontend build tool |
| `vite-plugin-singlefile` | ^2.3.2 | Inlines all assets into a single HTML file |
| `concurrently` | ^9.2.1 | Runs multiple npm scripts in parallel (dev mode) |
| `cross-env` | ^10.1.0 | Sets environment variables cross-platform |
| `@types/cors` | ^2.8.19 | TypeScript types for cors |
| `@types/express` | ^5.0.6 | TypeScript types for Express 5 |
| `@types/node` | ^25.5.0 | TypeScript types for Node.js |

### Implicit Dependencies (via MCP SDK)

- `zod` — Schema validation for MCP tool input definitions
- Various MCP protocol internals (JSON-RPC, transport abstractions)

---

## 11. Security Model

### Read-Only Enforcement

**Layer 1 — Upstream MCP server:** The `flow-microstrategy-prd-http` server only exposes a `read-cypher` tool. It's configured to reject write operations at the server level.

**Layer 2 — Local validation:** `tool-handlers.ts` applies a regex blocklist before sending any Cypher query:
```typescript
const WRITE_OPS_RE = /\b(CREATE|DELETE|SET|REMOVE|MERGE|DROP|DETACH)\b/i;
```

**Layer 3 — Bearer token:** The upstream connection uses a Bearer token with limited permissions.

### Iframe Sandbox

MCP App iframes use `sandbox="allow-scripts allow-same-origin"`:
- Scripts can run (required for the dashboard logic)
- Same-origin allows postMessage communication
- Navigation, popups, and forms are blocked

### Claude CLI Execution

The `--dangerously-skip-permissions` flag is used when spawning the claude CLI. This is necessary because the chat API runs non-interactively (no terminal for permission prompts). The system prompt constrains Claude to only use the 4 parity analysis tools.

### Cypher Injection

The `escCypher` function escapes single quotes in user input before embedding in Cypher queries:
```typescript
function escCypher(s: string): string {
  return s.replace(/'/g, "\\'");
}
```

This prevents Cypher injection via tool arguments. However, the primary defense is the read-only enforcement — even if injection succeeded, it could only execute read operations.

---

## 12. Key Design Decisions

### Why spawn the Claude CLI instead of calling the Anthropic API?

1. **No API key management** — the CLI uses the user's existing auth session
2. **Automatic MCP discovery** — Claude reads `.mcp.json` from CWD and connects to servers
3. **Complete agentic loop** — multi-turn tool calling is handled internally by the CLI
4. **Consistent experience** — same behavior as running Claude Code directly

Trade-off: Each message spawns a new process (~2-4 seconds startup). The conversation history is sent as a single prompt, so context grows linearly.

### Why single-file HTML builds?

MCP hosts serve app HTML as embedded resources. The host fetches the HTML via the MCP `resources/read` method and renders it in an iframe. External script references (CDN links, separate JS/CSS files) would fail because the iframe's effective origin is different from the original server.

`vite-plugin-singlefile` solves this by inlining everything into one HTML file.

### Why custom Canvas charts instead of Chart.js?

Chart.js would add ~200KB to the bundle and requires CSP (`Content-Security-Policy`) configuration for inline styles. The custom Canvas implementations (`drawDoughnut`, `drawRadar`) are ~150 lines total and produce the exact visualizations needed.

### Why both MCP server and direct HTTP tool execution?

The chat interface uses the Claude CLI, which connects to the MCP server via stdio. But when an iframe needs to call a tool (e.g., user clicks a report → triggers analysis), the iframe can't connect to the MCP server directly.

Solution: `POST /api/tool` — a simple HTTP endpoint that calls `executeTool()` directly, bypassing the MCP protocol. The iframe sends a postMessage to the parent chat, which proxies to this endpoint.

### Why Express 5?

Express 5 was chosen for its improved async error handling (no need for `try/catch` wrappers on async route handlers). The trade-off is that some middleware behavior differs from Express 4 — notably, `req.on("close")` fires prematurely with SSE, which required using `res.on("close")` instead.

### Why `tsx` instead of compiling TypeScript?

`tsx` provides zero-config TypeScript execution for Node.js. Since the server code changes frequently during development and the build step only matters for the frontend (Vite handles that), using `tsx` eliminates the server compilation step entirely. `tsc --noEmit` is used purely for type checking.
