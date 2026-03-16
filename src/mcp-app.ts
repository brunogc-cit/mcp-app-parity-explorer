/**
 * Client-side MCP App logic — Parity Mapping Explorer
 *
 * Runs inside a sandboxed iframe. Communicates with the MCP App server
 * via the ext-apps App class (JSON-RPC over postMessage).
 */

import { App } from "@modelcontextprotocol/ext-apps";
import "./styles.css";

// ═══════════════════════════════════════════════════════════════════════════
// Chart.js (loaded inline — bundled by vite-plugin-singlefile)
// ═══════════════════════════════════════════════════════════════════════════

// We'll use a minimal inline chart implementation since Chart.js CDN
// needs CSP declaration. For the PoC, we draw charts on canvas directly.

interface ChartData {
  labels: string[];
  values: number[];
  colors: string[];
}

function drawDoughnut(canvas: HTMLCanvasElement, data: ChartData, title?: string): void {
  const ctx = canvas.getContext("2d")!;
  const w = canvas.width = canvas.offsetWidth * 2;
  const h = canvas.height = canvas.offsetHeight * 2;
  ctx.scale(2, 2);
  const cw = w / 2;
  const ch = h / 2;
  const cx = cw / 2;
  const cy = ch / 2;
  const radius = Math.min(cx, cy) - 10;
  const innerRadius = radius * 0.55;
  const total = data.values.reduce((a, b) => a + b, 0);

  if (total === 0) {
    ctx.fillStyle = "#2e3345";
    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, Math.PI * 2);
    ctx.arc(cx, cy, innerRadius, 0, Math.PI * 2, true);
    ctx.fill();
    return;
  }

  let angle = -Math.PI / 2;
  data.values.forEach((val, i) => {
    const slice = (val / total) * Math.PI * 2;
    ctx.fillStyle = data.colors[i];
    ctx.beginPath();
    ctx.moveTo(cx + innerRadius * Math.cos(angle), cy + innerRadius * Math.sin(angle));
    ctx.arc(cx, cy, radius, angle, angle + slice);
    ctx.arc(cx, cy, innerRadius, angle + slice, angle, true);
    ctx.closePath();
    ctx.fill();
    angle += slice;
  });

  // Legend
  const legendX = cw / 2 + radius + 16;
  let legendY = 12;
  ctx.font = "11px -apple-system, sans-serif";
  data.labels.forEach((label, i) => {
    if (data.values[i] === 0) return;
    ctx.fillStyle = data.colors[i];
    ctx.fillRect(legendX, legendY, 10, 10);
    ctx.fillStyle = "#8b8fa3";
    ctx.fillText(`${label} (${data.values[i]})`, legendX + 14, legendY + 9);
    legendY += 16;
  });

  if (title) {
    ctx.fillStyle = "#e4e6ed";
    ctx.font = "bold 18px -apple-system, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(title, cx, cy + 6);
    ctx.textAlign = "start";
  }
}

function drawRadar(canvas: HTMLCanvasElement, values: number[], labels: string[]): void {
  const ctx = canvas.getContext("2d")!;
  const size = 240;
  canvas.width = size * 2;
  canvas.height = size * 2;
  ctx.scale(2, 2);
  const cx = size / 2;
  const cy = size / 2;
  const radius = 80;
  const n = labels.length;
  const angleStep = (Math.PI * 2) / n;

  // Grid
  for (let ring = 1; ring <= 4; ring++) {
    const r = (ring / 4) * radius;
    ctx.strokeStyle = "#2e3345";
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let i = 0; i <= n; i++) {
      const a = -Math.PI / 2 + i * angleStep;
      const x = cx + r * Math.cos(a);
      const y = cy + r * Math.sin(a);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  // Axes
  for (let i = 0; i < n; i++) {
    const a = -Math.PI / 2 + i * angleStep;
    ctx.strokeStyle = "#2e3345";
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(cx + radius * Math.cos(a), cy + radius * Math.sin(a));
    ctx.stroke();

    // Labels
    const lx = cx + (radius + 16) * Math.cos(a);
    const ly = cy + (radius + 16) * Math.sin(a);
    ctx.fillStyle = "#8b8fa3";
    ctx.font = "11px -apple-system, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(labels[i], lx, ly + 4);
  }

  // Data polygon
  ctx.fillStyle = "rgba(108, 138, 255, 0.2)";
  ctx.strokeStyle = "#6c8aff";
  ctx.lineWidth = 2;
  ctx.beginPath();
  values.forEach((v, i) => {
    const a = -Math.PI / 2 + i * angleStep;
    const r = v * radius;
    const x = cx + r * Math.cos(a);
    const y = cy + r * Math.sin(a);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.closePath();
  ctx.fill();
  ctx.stroke();

  // Data points
  values.forEach((v, i) => {
    const a = -Math.PI / 2 + i * angleStep;
    const r = v * radius;
    ctx.fillStyle = "#6c8aff";
    ctx.beginPath();
    ctx.arc(cx + r * Math.cos(a), cy + r * Math.sin(a), 3, 0, Math.PI * 2);
    ctx.fill();
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════════════════

interface MappingItem {
  guid: string;
  name: string;
  type: string;
  parity_status: string;
  score: number;
  level: string;
  match: { name: string; model: string } | null;
  signals: { s1: number; s2: number; s3: number; s4: number; s5: number };
}

interface AnalysisData {
  report_guid: string;
  summary: {
    total: number;
    confirmed: number;
    high: number;
    medium: number;
    low: number;
    unmapped: number;
    coverage: number;
    parity: Record<string, number>;
  };
  mappings: MappingItem[];
}

let currentData: AnalysisData | null = null;
let activeFilter = "All";
let activeType = "All";
let sortCol = "score";
let sortDir: "asc" | "desc" = "desc";

// ═══════════════════════════════════════════════════════════════════════════
// DOM refs
// ═══════════════════════════════════════════════════════════════════════════

const searchInput = document.getElementById("search-input") as HTMLInputElement;
const searchBtn = document.getElementById("search-btn")!;
const searchResults = document.getElementById("search-results")!;
const loadingEl = document.getElementById("loading")!;
const dashboardEl = document.getElementById("dashboard")!;
const emptyState = document.getElementById("empty-state")!;
const tbody = document.getElementById("mapping-tbody")!;
const detailPanel = document.getElementById("detail-panel")!;
const detailTitle = document.getElementById("detail-title")!;
const detailGrid = document.getElementById("detail-grid")!;

// ═══════════════════════════════════════════════════════════════════════════
// MCP App Connection
// ═══════════════════════════════════════════════════════════════════════════

const app = new App({ name: "Parity Mapping Explorer", version: "1.0.0" });

// Handle tool results pushed by the host (when LLM calls a tool)
app.ontoolresult = (result) => {
  const content = result.content?.find(
    (c: { type: string; text?: string }) => c.type === "text",
  );
  if (!content || !("text" in content)) return;

  try {
    const data = JSON.parse(content.text as string);
    handleServerData(data);
  } catch {
    console.error("Failed to parse tool result");
  }
};

app.connect();

// ═══════════════════════════════════════════════════════════════════════════
// Event Handlers
// ═══════════════════════════════════════════════════════════════════════════

searchBtn.addEventListener("click", doSearch);
searchInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doSearch();
});

// Filter chips
document.querySelectorAll(".filter-chip[data-level]").forEach((chip) => {
  chip.addEventListener("click", () => {
    document.querySelectorAll(".filter-chip[data-level]").forEach((c) => c.classList.remove("active"));
    chip.classList.add("active");
    activeFilter = (chip as HTMLElement).dataset.level!;
    renderTable();
  });
});

document.querySelectorAll(".filter-chip[data-type]").forEach((chip) => {
  chip.addEventListener("click", () => {
    document.querySelectorAll(".filter-chip[data-type]").forEach((c) => c.classList.remove("active"));
    chip.classList.add("active");
    activeType = (chip as HTMLElement).dataset.type!;
    renderTable();
  });
});

// Sort headers
document.querySelectorAll("thead th[data-sort]").forEach((th) => {
  th.addEventListener("click", () => {
    const col = (th as HTMLElement).dataset.sort!;
    if (sortCol === col) {
      sortDir = sortDir === "asc" ? "desc" : "asc";
    } else {
      sortCol = col;
      sortDir = "desc";
    }
    renderTable();
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// Actions
// ═══════════════════════════════════════════════════════════════════════════

async function doSearch(): Promise<void> {
  const query = searchInput.value.trim();
  if (!query) return;

  searchResults.classList.remove("hidden");
  searchResults.innerHTML = '<div class="loading">Searching</div>';

  try {
    const result = await app.callServerTool({
      name: "search-reports",
      arguments: { query },
    });
    const content = result.content?.find(
      (c: { type: string; text?: string }) => c.type === "text",
    );
    if (content && "text" in content) {
      const data = JSON.parse(content.text as string);
      handleServerData(data);
    }
  } catch (err) {
    searchResults.innerHTML = `<div class="empty">Error: ${err}</div>`;
  }
}

async function analyzeReport(guid: string): Promise<void> {
  searchResults.classList.add("hidden");
  emptyState.classList.add("hidden");
  dashboardEl.classList.add("hidden");
  detailPanel.classList.remove("visible");
  loadingEl.classList.remove("hidden");

  try {
    const result = await app.callServerTool({
      name: "run-parity-analysis",
      arguments: { guid },
    });
    const content = result.content?.find(
      (c: { type: string; text?: string }) => c.type === "text",
    );
    if (content && "text" in content) {
      const data = JSON.parse(content.text as string);
      handleServerData(data);
    }
  } catch (err) {
    loadingEl.classList.add("hidden");
    emptyState.classList.remove("hidden");
    emptyState.textContent = `Error: ${err}`;
  }
}

async function showDetail(item: MappingItem): Promise<void> {
  detailPanel.classList.add("visible");
  detailTitle.textContent = `${item.name} (${item.type})`;

  const fields = [
    { label: "GUID", value: item.guid },
    { label: "Parity Status", value: item.parity_status },
    { label: "PBI Match", value: item.match ? `${item.match.name} (${item.match.model})` : "None" },
    { label: "Final Score", value: `${(item.score * 100).toFixed(1)}%` },
    { label: "Confidence", value: item.level },
    { label: "S1 Direct", value: item.signals.s1 > 0 ? `${(item.signals.s1 * 100).toFixed(0)}%` : "-" },
    { label: "S2 Lineage", value: item.signals.s2 > 0 ? `${(item.signals.s2 * 100).toFixed(0)}%` : "-" },
    { label: "S3 Name", value: item.signals.s3 > 0 ? `${(item.signals.s3 * 100).toFixed(0)}%` : "-" },
    { label: "S4 Formula", value: item.signals.s4 > 0 ? `${(item.signals.s4 * 100).toFixed(0)}%` : "-" },
    { label: "S5 Context", value: item.signals.s5 > 0 ? `${(item.signals.s5 * 100).toFixed(0)}%` : "-" },
  ];

  detailGrid.innerHTML = fields
    .map((f) => `<div class="detail-field"><div class="label">${f.label}</div><div>${f.value}</div></div>`)
    .join("");

  // Radar chart
  const radarCanvas = document.getElementById("radar-chart") as HTMLCanvasElement;
  drawRadar(
    radarCanvas,
    [item.signals.s1, item.signals.s2, item.signals.s3, item.signals.s4, item.signals.s5],
    ["S1 Direct", "S2 Lineage", "S3 Name", "S4 Formula", "S5 Context"],
  );

  // Update model context with selection
  await app.updateModelContext({
    content: [
      {
        type: "text",
        text: `User selected: ${item.name} (${item.type}, ${item.level}, score=${item.score})${item.match ? ` → PBI: ${item.match.name} (${item.match.model})` : " → Unmapped"}`,
      },
    ],
  });

  // Scroll to detail panel
  detailPanel.scrollIntoView({ behavior: "smooth" });
}

// ═══════════════════════════════════════════════════════════════════════════
// Data Handler
// ═══════════════════════════════════════════════════════════════════════════

function handleServerData(data: { action: string; [key: string]: unknown }): void {
  switch (data.action) {
    case "search-results":
      renderSearchResults(data.data as Array<{ guid: string; name: string; subtype: string; location: string }>);
      break;

    case "parity-analysis":
      loadingEl.classList.add("hidden");
      currentData = data as unknown as AnalysisData;
      renderDashboard();
      break;

    case "detail":
      // Handle detail response
      break;

    case "summary":
      // Handle summary response
      break;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Renderers
// ═══════════════════════════════════════════════════════════════════════════

function renderSearchResults(
  results: Array<{ guid: string; name: string; subtype: string; location: string }>,
): void {
  if (!results || results.length === 0) {
    searchResults.innerHTML = '<div class="empty">No reports found</div>';
    return;
  }

  searchResults.innerHTML = results
    .map(
      (r) => `
    <div class="result-item" data-guid="${r.guid}">
      <div>
        <div class="name">${escapeHtml(r.name)}</div>
        <div class="guid">${r.guid}</div>
      </div>
      <div style="font-size:11px;color:var(--text-dim)">${escapeHtml(r.subtype || "")}</div>
    </div>
  `,
    )
    .join("");

  searchResults.querySelectorAll(".result-item").forEach((item) => {
    item.addEventListener("click", () => {
      const guid = (item as HTMLElement).dataset.guid!;
      analyzeReport(guid);
    });
  });
}

function renderDashboard(): void {
  if (!currentData) return;

  dashboardEl.classList.remove("hidden");
  emptyState.classList.add("hidden");

  const { summary } = currentData;

  // KPIs
  document.getElementById("kpi-total")!.textContent = String(summary.total);
  document.getElementById("kpi-mapped")!.textContent = String(summary.total - summary.unmapped);
  document.getElementById("kpi-coverage")!.textContent = `${summary.coverage}%`;
  document.getElementById("kpi-confirmed")!.textContent = String(summary.confirmed);
  document.getElementById("kpi-unmapped")!.textContent = String(summary.unmapped);

  // Confidence donut
  const confCanvas = document.getElementById("confidence-chart") as HTMLCanvasElement;
  drawDoughnut(confCanvas, {
    labels: ["Confirmed", "High", "Medium", "Low", "Unmapped"],
    values: [summary.confirmed, summary.high, summary.medium, summary.low, summary.unmapped],
    colors: ["#22c55e", "#3b82f6", "#f59e0b", "#ef4444", "#6b7280"],
  }, `${summary.coverage}%`);

  // Parity pie
  const parityCanvas = document.getElementById("parity-chart") as HTMLCanvasElement;
  const parity = summary.parity;
  drawDoughnut(parityCanvas, {
    labels: ["Complete", "Planned", "Drop", "Not Planned", "Unknown"],
    values: [
      parity["Complete"] || 0,
      parity["Planned"] || 0,
      parity["Drop"] || 0,
      parity["Not Planned"] || 0,
      parity["Unknown"] || 0,
    ],
    colors: ["#22c55e", "#f59e0b", "#ef4444", "#6b7280", "#4b5563"],
  });

  renderTable();
}

function renderTable(): void {
  if (!currentData) return;

  let items = [...currentData.mappings];

  // Filter by confidence level
  if (activeFilter !== "All") {
    items = items.filter((m) => m.level === activeFilter);
  }

  // Filter by type
  if (activeType !== "All") {
    items = items.filter((m) => m.type === activeType || m.type === "DerivedMetric" && activeType === "Metric");
  }

  // Sort
  items.sort((a, b) => {
    let va: string | number, vb: string | number;
    switch (sortCol) {
      case "name": va = a.name.toLowerCase(); vb = b.name.toLowerCase(); break;
      case "type": va = a.type; vb = b.type; break;
      case "match": va = a.match?.name || ""; vb = b.match?.name || ""; break;
      case "score": va = a.score; vb = b.score; break;
      case "level": {
        const order: Record<string, number> = { Confirmed: 5, High: 4, Medium: 3, Low: 2, Unmapped: 1 };
        va = order[a.level] || 0; vb = order[b.level] || 0; break;
      }
      default: va = a.score; vb = b.score;
    }
    if (va < vb) return sortDir === "asc" ? -1 : 1;
    if (va > vb) return sortDir === "asc" ? 1 : -1;
    return 0;
  });

  tbody.innerHTML = items
    .map(
      (m) => `
    <tr data-guid="${m.guid}">
      <td title="${escapeHtml(m.guid)}">${escapeHtml(m.name)}</td>
      <td>${m.type}</td>
      <td>${m.match ? escapeHtml(m.match.name) : '<span style="color:var(--text-dim)">-</span>'}</td>
      <td>
        <span class="score-bar"><span class="score-bar-fill" style="width:${m.score * 100}%;background:${scoreColor(m.level)}"></span></span>
        ${(m.score * 100).toFixed(0)}%
      </td>
      <td><span class="badge-level ${m.level}">${m.level}</span></td>
      <td>${renderSignalDots(m.signals)}</td>
    </tr>
  `,
    )
    .join("");

  // Row click → detail
  tbody.querySelectorAll("tr").forEach((tr) => {
    tr.addEventListener("click", () => {
      const guid = (tr as HTMLElement).dataset.guid!;
      const item = currentData!.mappings.find((m) => m.guid === guid);
      if (item) showDetail(item);
    });
  });
}

function renderSignalDots(signals: { s1: number; s2: number; s3: number; s4: number; s5: number }): string {
  const dots = [
    { key: "s1", val: signals.s1, cls: signals.s1 > 0 ? "active s1" : "" },
    { key: "s2", val: signals.s2, cls: signals.s2 > 0 ? "active" : "" },
    { key: "s3", val: signals.s3, cls: signals.s3 > 0 ? "active" : "" },
    { key: "s4", val: signals.s4, cls: signals.s4 > 0 ? "active" : "" },
    { key: "s5", val: signals.s5, cls: signals.s5 > 0 ? "active" : "" },
  ];
  return `<div class="signal-dots" title="S1:${signals.s1.toFixed(2)} S2:${signals.s2.toFixed(2)} S3:${signals.s3.toFixed(2)} S4:${signals.s4.toFixed(2)} S5:${signals.s5.toFixed(2)}">${dots.map((d) => `<div class="signal-dot ${d.cls}" title="${d.key}: ${d.val.toFixed(2)}"></div>`).join("")}</div>`;
}

function scoreColor(level: string): string {
  switch (level) {
    case "Confirmed": return "#22c55e";
    case "High": return "#3b82f6";
    case "Medium": return "#f59e0b";
    case "Low": return "#ef4444";
    default: return "#6b7280";
  }
}

function escapeHtml(str: string): string {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

// ═══════════════════════════════════════════════════════════════════════════
// PostMessage listener — for embedding in chat interface iframes
// When there's no MCP App host, data can be injected via postMessage.
// ═══════════════════════════════════════════════════════════════════════════

window.addEventListener("message", (event) => {
  if (event.data?.type === "inject-tool-result") {
    try {
      const data = typeof event.data.data === "string" ? JSON.parse(event.data.data) : event.data.data;
      handleServerData(data);
    } catch (e) {
      console.error("Failed to handle injected tool result:", e);
    }
  }
  // Handle tool request response from parent chat
  if (event.data?.type === "tool-response") {
    try {
      const data = typeof event.data.data === "string" ? JSON.parse(event.data.data) : event.data.data;
      handleServerData(data);
    } catch (e) {
      console.error("Failed to handle tool response:", e);
    }
  }
});

// Override callServerTool when in chat iframe mode (no MCP host)
const isInChatIframe = window.parent !== window;
if (isInChatIframe) {
  const originalCallServerTool = app.callServerTool.bind(app);

  app.callServerTool = async (params: { name: string; arguments?: Record<string, unknown> }) => {
    try {
      // Try MCP host first
      return await Promise.race([
        originalCallServerTool(params),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error("timeout")), 2000)),
      ]);
    } catch {
      // Fallback: ask parent chat to execute the tool
      window.parent.postMessage({
        type: "tool-request",
        name: params.name,
        arguments: params.arguments,
      }, "*");
      // Return empty — parent will inject result via postMessage
      return { content: [] };
    }
  };
}
