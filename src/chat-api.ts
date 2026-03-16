/**
 * Chat API — spawns the local `claude` CLI to handle conversations.
 * Claude discovers MCP tools (parity-explorer) via .mcp.json in the CWD.
 * Streams responses back as Server-Sent Events (SSE).
 */

import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Express, Request, Response } from "express";
import { executeTool } from "./tool-handlers.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Directory containing .mcp.json with parity-explorer config
const MCP_CONFIG_DIR = path.resolve(__dirname, "../../../asos-agentic-workflow");
const WORKSPACE_ROOT = path.resolve(__dirname, "../../..");

// System prompt for the parity analysis assistant
const SYSTEM_PROMPT = `You are a Parity Analysis assistant for MSTR-to-PBI migration. You help users explore MicroStrategy reports and their Power BI mapping status.

Available tools via MCP:
- search-reports: Search for MicroStrategy reports by name or GUID
- run-parity-analysis: Run full 5-signal parity analysis on a report
- get-mapping-detail: Get detailed signal breakdown for a specific metric/attribute
- get-parity-summary: Get aggregate parity stats for a report

When a user asks about a report:
1. Search for it first using search-reports
2. Present the results and offer to run analysis
3. When they confirm, use run-parity-analysis with the GUID

Always use tools to get data — never make up report names or GUIDs.
Keep responses concise. The tool results will be rendered as interactive UI.
Respond in the same language as the user (Portuguese or English).`;

function findClaudeBinary(): string {
  // Set CLAUDE_PATH if `claude` is not on your PATH (e.g. ~/.local/bin/claude)
  return process.env.CLAUDE_PATH || "claude";
}

/**
 * Write an SSE event to the response.
 */
function sendSSE(res: Response, event: string, data: unknown): void {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

/**
 * Format conversation messages into a prompt for claude --print.
 * Since --print is stateless, we include the full conversation context.
 */
function formatPrompt(messages: Array<{ role: string; content: string }>): string {
  if (messages.length === 1) {
    return messages[0].content;
  }

  const history = messages.slice(0, -1).map((m) => {
    const prefix = m.role === "user" ? "User" : "Assistant";
    return `${prefix}: ${m.content}`;
  }).join("\n\n");

  const lastMessage = messages[messages.length - 1].content;

  return `Previous conversation:\n${history}\n\nCurrent question:\n${lastMessage}`;
}

/**
 * Parse a stream-json line from claude --print --output-format stream-json.
 *
 * Events:
 * - {"type":"system","subtype":"init",...}
 * - {"type":"assistant","message":{content:[{type:"text",...},{type:"tool_use",...}]}}
 * - {"type":"user","message":{content:[{type:"tool_result",...}]},"tool_use_result":{...}}
 * - {"type":"result","is_error":bool,"total_cost_usd":num,...}
 */
interface StreamEvent {
  type: string;
  subtype?: string;
  message?: {
    content?: Array<{
      type: string;
      text?: string;
      name?: string;
      id?: string;
      input?: Record<string, unknown>;
      tool_use_id?: string;
      content?: unknown;
    }>;
  };
  tool_use_result?: { stdout?: string; stderr?: string };
  is_error?: boolean;
  total_cost_usd?: number;
  duration_ms?: number;
  num_turns?: number;
}

export function registerChatRoutes(app: Express): void {
  // ─── POST /api/chat — SSE streaming chat ────────────────────────────
  app.post("/api/chat", async (req: Request, res: Response) => {
    const { messages } = req.body as { messages: Array<{ role: string; content: string }> };

    if (!messages || messages.length === 0) {
      res.status(400).json({ error: "messages array required" });
      return;
    }

    // SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();

    const prompt = formatPrompt(messages);
    const claudeBin = findClaudeBinary();

    let proc: ChildProcess;
    try {
      // Remove all Claude Code env vars to avoid nested session detection
      const env = { ...process.env };
      for (const key of Object.keys(env)) {
        if (key.startsWith("CLAUDE")) delete env[key];
      }

      proc = spawn(claudeBin, [
        "--print",
        "--output-format", "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
        "--system-prompt", SYSTEM_PROMPT,
        prompt,
      ], {
        cwd: MCP_CONFIG_DIR,
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch (err) {
      sendSSE(res, "error", { message: `Failed to spawn claude: ${err}` });
      sendSSE(res, "done", {});
      res.end();
      return;
    }

    // Track text accumulation for partial updates
    let lastAssistantText = "";

    const rl = createInterface({ input: proc.stdout! });

    rl.on("line", (line) => {
      if (!line.trim()) return;

      let event: StreamEvent;
      try {
        event = JSON.parse(line);
      } catch {
        return; // Skip non-JSON lines
      }

      switch (event.type) {
        case "system":
          // Init event — can send model info
          if (event.subtype === "init") {
            sendSSE(res, "system", { subtype: "init" });
          }
          break;

        case "assistant":
          if (event.message?.content) {
            for (const block of event.message.content) {
              if (block.type === "text" && block.text) {
                // Send new text delta (diff from last)
                const newText = block.text;
                if (newText !== lastAssistantText) {
                  const delta = newText.slice(lastAssistantText.length);
                  if (delta) {
                    sendSSE(res, "delta", { text: delta });
                  }
                  lastAssistantText = newText;
                }
              }
              if (block.type === "tool_use") {
                // Reset text for next assistant turn
                lastAssistantText = "";
                sendSSE(res, "tool_call", {
                  id: block.id,
                  name: block.name,
                  input: block.input,
                });
              }
            }
          }
          break;

        case "user":
          // Tool result — extract the content
          if (event.message?.content) {
            for (const block of event.message.content) {
              if (block.type === "tool_result" && block.content) {
                const content = Array.isArray(block.content) ? block.content : [block.content];
                for (const item of content) {
                  const textItem = typeof item === "string" ? item : (item as { type: string; text?: string }).text;
                  if (textItem) {
                    try {
                      const data = JSON.parse(textItem);
                      sendSSE(res, "tool_result", {
                        id: block.tool_use_id,
                        data,
                        hasUi: true,
                      });
                    } catch {
                      sendSSE(res, "tool_result", {
                        id: block.tool_use_id,
                        data: { text: textItem },
                        hasUi: false,
                      });
                    }
                  }
                }
              }
            }
          }
          break;

        case "result":
          sendSSE(res, "done", {
            cost: event.total_cost_usd,
            duration: event.duration_ms,
            turns: event.num_turns,
            error: event.is_error,
          });
          break;
      }
    });

    // Capture stderr for debugging
    let stderr = "";
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code) => {
      if (code !== 0 && stderr) {
        sendSSE(res, "error", { message: stderr.slice(0, 500) });
      }
      sendSSE(res, "done", {});
      res.end();
    });

    proc.on("error", (err) => {
      sendSSE(res, "error", { message: `Process error: ${err.message}` });
      res.end();
    });

    // Handle client disconnect — use res "close" not req "close"
    // (req "close" fires prematurely in Express 5)
    res.on("close", () => {
      if (!res.writableEnded) proc.kill("SIGTERM");
    });
  });

  // ─── POST /api/tool — Direct tool execution (for iframe follow-ups) ──
  app.post("/api/tool", async (req: Request, res: Response) => {
    const { name, arguments: args } = req.body as { name: string; arguments: Record<string, unknown> };

    if (!name) {
      res.status(400).json({ error: "tool name required" });
      return;
    }

    try {
      const result = await executeTool(name, args || {}, WORKSPACE_ROOT);
      const text = result.content[0]?.text;
      const data = text ? JSON.parse(text) : null;
      res.json({ data });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });
}
