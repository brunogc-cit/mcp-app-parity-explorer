# MCP App Parity Explorer

Interactive parity mapping explorer for **MicroStrategy-to-Power BI** migration analysis. Built as an [MCP App](https://modelcontextprotocol.io/specification/2025-06-18/server/utilities/mcp-apps) with an embedded chat interface powered by the local `claude` CLI.

![MCP App PoC](https://img.shields.io/badge/MCP_App-PoC-6c8aff) ![READ-ONLY](https://img.shields.io/badge/Neo4j-READ--ONLY-22c55e) ![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178c6)

---

## What it does

Analyses how well MicroStrategy (MSTR) metrics and attributes map to their Power BI (PBI) equivalents using a **5-signal matching algorithm**:

| Signal | Name | Weight | What it checks |
|--------|------|--------|----------------|
| **S1** | Direct Neo4j Mapping | Authoritative (1.0) | Manually verified `pb_semantic_name` property in Neo4j |
| **S2** | Column Lineage | 0.30 | MSTR `ade_db_column` traced through dbt to PBI `sourceColumn` |
| **S3** | Name Similarity | 0.35 | Normalised Levenshtein + Jaccard with domain transforms |
| **S4** | Formula Analysis | 0.25 | MSTR formula structure vs DAX expression (metrics only) |
| **S5** | Table Context | 0.10 | Shared source tables between MSTR lineage and PBI partitions |

Results are classified into confidence levels: **Confirmed** (>=90%), **High** (>=70%), **Medium** (>=50%), **Low** (>=30%), **Unmapped** (<30%).

---

## Architecture

```
Browser (localhost:3001)
├── /chat.html ............... Chat interface (natural language)
│   ├── POST /api/chat ....... SSE stream → spawns claude CLI
│   └── Embedded iframes ..... mcp-app.html with interactive UI
│
├── /mcp-app.html ............ Standalone MCP App (basic-host / Claude Desktop)
│   └── Charts, tables, filters, radar chart, detail panel
│
└── /mcp ..................... MCP protocol endpoint (HTTP transport)

Express Server (port 3001)
├── /api/chat ................ SSE streaming → claude --print --stream-json
├── /api/tool ................ Direct tool execution (iframe follow-ups)
├── /mcp ..................... MCP protocol (stdio + HTTP transports)
└── static files ............. dist/ (Vite-built single-file HTML)
```

### Data flow

```
User types question
  → POST /api/chat (SSE)
    → spawn claude CLI with --print --output-format stream-json
      → Claude discovers MCP tools via .mcp.json
      → Claude calls search-reports / run-parity-analysis / etc.
        → Tool handlers query Neo4j (READ-ONLY) + scan PBI models
      → Stream-json events parsed and forwarded as SSE
    → Browser renders text as chat bubbles
    → Tool results rendered as embedded mcp-app.html iframes
      → Iframe receives data via postMessage
      → Charts, tables, and filters render inside iframe
```

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Node.js** | >= 18.x | v20+ recommended |
| **Claude CLI** | >= 2.x | `claude` must be on PATH, or set `CLAUDE_PATH` env var |
| **Neo4j** | (remote) | Upstream MCP server provides read-only access |
| **PBI models** | local | `asos-data-ade-powerbi/powerbi/models/` in the workspace |

### Claude CLI

The chat interface uses the locally installed [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) to process natural language queries. It does **not** require an `ANTHROPIC_API_KEY` — it uses your authenticated CLI session.

Install Claude Code if you haven't:

```bash
# macOS / Linux
curl -fsSL https://claude.ai/install.sh | sh

# Or via npm
npm install -g @anthropic-ai/claude-code
```

Verify it's working:

```bash
claude --version
# 2.1.76 (Claude Code)
```

If `claude` is not on your PATH (e.g., installed at `~/.local/bin/claude`), set the environment variable:

```bash
export CLAUDE_PATH="$HOME/.local/bin/claude"
```

---

## Installation

```bash
cd pocs/mcp-app-parity-explorer
npm install
```

---

## Building

```bash
npm run build
```

This runs:
1. `tsc --noEmit` — type checking
2. `tsc -p tsconfig.server.json` — emit server type declarations
3. `vite build` for `mcp-app.html` — bundles into a single self-contained HTML file
4. `vite build` for `chat.html` — bundles the chat interface

Output: `dist/mcp-app.html` (~136KB) and `dist/chat.html` (~12KB).

Both HTML files are built with [vite-plugin-singlefile](https://github.com/nicolo-ribaudo/vite-plugin-singlefile), which inlines all JS and CSS — no external dependencies at runtime. This is required for MCP App compatibility (the HTML must be fully self-contained).

---

## Running

### Development mode (with hot reload)

```bash
npm start
```

This starts three concurrent processes:
- Vite watch for `mcp-app.html`
- Vite watch for `chat.html`
- Express server via `tsx watch main.ts`

### Production mode

```bash
npm run build
npx tsx main.ts
```

### Server URLs

| URL | Description |
|-----|-------------|
| `http://localhost:3001/` | Redirects to chat interface |
| `http://localhost:3001/chat.html` | Chat interface (natural language) |
| `http://localhost:3001/mcp-app.html` | Standalone MCP App UI |
| `http://localhost:3001/mcp` | MCP protocol endpoint (HTTP transport) |

The port defaults to `3001`. Override with `PORT` environment variable:

```bash
PORT=8080 npx tsx main.ts
```

---

## Usage

### 1. Chat Interface (recommended)

Open http://localhost:3001 in your browser. Type natural language questions:

- "Search for the Monday Huddle report"
- "Pesquisa reports de Sales"
- "Run parity analysis for report GUID 2806F1C6..."

The assistant will:
1. Search for matching reports via the `search-reports` MCP tool
2. Present results as interactive UI in an embedded iframe
3. Offer to run full 5-signal parity analysis
4. Show charts (confidence distribution, parity status), filterable tables, and signal radar charts

**Chat features:**
- Streaming text responses (SSE)
- Tool call notifications with status indicators
- Embedded interactive iframes for tool results (search results, parity dashboard, signal detail)
- Collapsible iframe sections
- Cost and duration display per response
- Suggestion buttons for common queries
- Multi-turn conversation context

### 2. Claude Desktop

Add to your Claude Desktop MCP config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "parity-explorer": {
      "command": "/absolute/path/to/node",
      "args": [
        "/absolute/path/to/node_modules/.bin/tsx",
        "/absolute/path/to/pocs/mcp-app-parity-explorer/main.ts",
        "--stdio"
      ],
      "cwd": "/absolute/path/to/pocs/mcp-app-parity-explorer"
    }
  }
}
```

> **Important:** Claude Desktop on macOS doesn't load your shell profile (`.zshrc`, nvm, etc.). You must use **absolute paths** to both the Node binary and the project files. Find your Node path with `which node` or `~/.nvm/versions/node/v20.x.x/bin/node`.

After saving, restart Claude Desktop. You'll see 4 new tools available:
- `search-reports` — Search reports by name or GUID
- `run-parity-analysis` — Full 5-signal analysis with interactive dashboard
- `get-mapping-detail` — Signal breakdown for a specific metric/attribute
- `get-parity-summary` — Aggregate parity statistics

Ask Claude: "Search for the Monday Huddle report" — results appear as an interactive MCP App iframe.

### 3. MCP basic-host (developer testing)

The [basic-host](https://github.com/nicolo-ribaudo/mcp-basic-host) tool connects to the HTTP transport for testing:

```bash
npx @nicolo-ribaudo/mcp-basic-host http://localhost:3001/mcp
```

> **Note:** basic-host requires **JSON input**, not natural language. For `search-reports`, type:
> ```json
> {"query": "Monday Huddle"}
> ```

### 4. Claude Code (terminal)

Add to your workspace `.mcp.json`:

```json
{
  "mcpServers": {
    "parity-explorer": {
      "command": "npx",
      "args": ["tsx", "main.ts", "--stdio"],
      "cwd": "/absolute/path/to/pocs/mcp-app-parity-explorer"
    }
  }
}
```

> **Note:** Claude Code terminal supports MCP tools but does **not** render MCP App UIs (iframes). You'll get the JSON data but without the interactive charts and tables.

---

## MCP App Client Compatibility

| Client | MCP Tools | MCP App UI (iframe) |
|--------|-----------|---------------------|
| **Claude Desktop** | Yes | Yes |
| **ChatGPT** | Yes | Yes |
| **VS Code Copilot** | Yes | Yes |
| **Goose** | Yes | Yes |
| **Postman** | Yes | Yes |
| **Cursor** | Yes | No (tools only) |
| **Claude Code (terminal)** | Yes | No (tools only) |
| **Claude Web (claude.ai)** | No | No (no MCP support) |
| **Chat Interface (this app)** | Yes | Yes |

---

## MCP Tools

### `search-reports`

Search MicroStrategy reports by name (partial match) or GUID.

**Input:**
```json
{ "query": "Monday Huddle" }
```

**Output:** List of matching reports with GUID, name, subtype, and location. Rendered as clickable cards in the MCP App UI.

### `run-parity-analysis`

Run full 5-signal parity analysis for all metrics and attributes in a report.

**Input:**
```json
{ "guid": "2806F1C6ABCD1234EF567890ABCDEF12" }
```

**Output:** Complete analysis dashboard with:
- KPIs (total objects, mapped, coverage %, confirmed, unmapped)
- Confidence distribution doughnut chart
- Parity status doughnut chart
- Sortable/filterable mapping table
- Click any row for signal radar chart and detail breakdown

### `get-mapping-detail`

Get detailed signal breakdown for a specific MSTR metric or attribute.

**Input:**
```json
{ "guid": "METRIC_GUID_HERE" }
```

**Output:** Individual mapping analysis with all 5 signal scores, best PBI match, and confidence classification.

### `get-parity-summary`

Get aggregate parity status counts for a report.

**Input:**
```json
{ "guid": "REPORT_GUID_HERE" }
```

**Output:** Summary of parity statuses (Complete, Planned, Drop, Not Planned, Unknown) across all objects in the report.

---

## Project Structure

```
mcp-app-parity-explorer/
├── main.ts .................. Entry point (HTTP + stdio transports)
├── server.ts ................ MCP server definition (tools + UI resource)
├── chat.html ................ Chat interface HTML entry point
├── mcp-app.html ............. MCP App HTML entry point
├── package.json
├── tsconfig.json ............ Type checking (noEmit)
├── tsconfig.server.json ..... Server type declarations
├── vite.config.ts ........... Vite build config (singlefile plugin)
├── src/
│   ├── chat-api.ts .......... Chat API routes (SSE + tool execution)
│   ├── chat.ts .............. Chat client-side logic
│   ├── chat-styles.css ...... Chat UI dark theme
│   ├── mcp-app.ts ........... MCP App client-side logic (charts, tables)
│   ├── styles.css ........... MCP App styles
│   ├── signals.ts ........... 5-signal matching algorithm (S1-S5)
│   ├── tool-defs.ts ......... UI tool name registry
│   └── tool-handlers.ts ..... Shared tool logic (Neo4j, PBI scanner)
├── skills/
│   └── parity-mapping/ ...... Skill definition for Claude agents
│       ├── SKILL.md ......... Skill prompt and instructions
│       ├── evals/ ........... Evaluation configurations
│       ├── references/ ...... Neo4j schema, sample output, tuning guide
│       └── scripts/ ......... Python analysis tools
└── dist/ .................... Built output (git-ignored)
    ├── chat.html ............ Self-contained chat interface
    └── mcp-app.html ......... Self-contained MCP App
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3001` | HTTP server port |
| `CLAUDE_PATH` | `claude` | Absolute path to the claude CLI binary |

### MCP Config Directory

The chat API spawns `claude` with its CWD set to a directory containing `.mcp.json`. By default, this is `../../../asos-agentic-workflow` relative to `src/chat-api.ts`. The `.mcp.json` in that directory must include the parity-explorer MCP server config so that Claude discovers the tools.

Example `.mcp.json`:

```json
{
  "mcpServers": {
    "parity-explorer": {
      "command": "npx",
      "args": ["tsx", "main.ts", "--stdio"],
      "cwd": "/absolute/path/to/pocs/mcp-app-parity-explorer"
    }
  }
}
```

### PBI Models

The parity analysis scans Power BI semantic model definitions from:

```
{workspace_root}/asos-data-ade-powerbi/powerbi/models/*/database.json
```

Each `database.json` contains tables with measures (DAX expressions) and columns (source mappings) used for signals S2, S3, S4, and S5.

### Upstream Neo4j (READ-ONLY)

Tool handlers connect to an upstream MCP server that provides read-only Neo4j access. The connection details are in `src/tool-handlers.ts`. All Cypher queries are validated against a write-operation blocklist (`CREATE`, `DELETE`, `SET`, `REMOVE`, `MERGE`, `DROP`, `DETACH`).

---

## Troubleshooting

### `spawn claude ENOENT`

The `claude` CLI binary is not found. Either:
1. Install Claude Code: `curl -fsSL https://claude.ai/install.sh | sh`
2. Set the full path: `export CLAUDE_PATH="$HOME/.local/bin/claude"`

### Chat returns empty responses

Check if:
- Claude CLI is authenticated (`claude --version` should work)
- The MCP config directory exists and contains `.mcp.json`
- No `CLAUDE*` env vars are interfering (the server strips them, but check if any other process is setting them)

### Claude Desktop: "Server disconnected"

Common causes:
- **Node version too old:** Claude Desktop uses system Node, not nvm. Use absolute path to Node >= 18.
- **Relative paths:** macOS GUI apps don't load `.zshrc`. Always use absolute paths in the config.
- **Missing `cwd`:** The `cwd` field is required for the server to find its dependencies.

### MCP App iframe shows blank

- Ensure `npm run build` has been run (dist/ must contain `mcp-app.html`)
- Check browser console for errors (iframe sandbox, CSP violations)
- The iframe receives data via `postMessage` with a 500ms delay after load — if the MCP App JS hasn't initialized yet, data may be lost. Refresh the page.

### PBI models not found

The scanner looks for `asos-data-ade-powerbi/powerbi/models/` relative to the workspace root. If the directory doesn't exist, the PBI index will be empty and signals S2-S5 will have no PBI candidates to match against. S1 (direct Neo4j mapping) will still work.

---

## Tech Stack

- **Runtime:** Node.js + [tsx](https://github.com/privatenumber/tsx) (TypeScript execution)
- **Server:** [Express 5](https://expressjs.com/) + [MCP SDK](https://github.com/modelcontextprotocol/typescript-sdk)
- **MCP Apps:** [@modelcontextprotocol/ext-apps](https://www.npmjs.com/package/@modelcontextprotocol/ext-apps) (tool registration + UI resources)
- **Frontend:** Vanilla TypeScript + [Vite](https://vite.dev/) + [vite-plugin-singlefile](https://github.com/nicolo-ribaudo/vite-plugin-singlefile)
- **Charts:** Custom Canvas 2D (doughnut charts, radar charts) — no external chart library
- **Chat backend:** `claude` CLI via `child_process.spawn` with `--output-format stream-json`

---

## License

ISC
