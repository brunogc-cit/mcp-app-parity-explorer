/**
 * Entry point — supports both stdio (Claude Desktop) and HTTP (browser) transports.
 */

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import express from "express";
import type { Request, Response } from "express";
import cors from "cors";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "./server.js";
import { registerChatRoutes } from "./src/chat-api.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function startStreamableHTTPServer(
  factory: () => McpServer,
): Promise<void> {
  const port = parseInt(process.env.PORT ?? "3001", 10);
  const app = express();
  app.use(cors());

  // ─── Static files (dist/) — must come before json parser ──────────
  app.use(express.static(path.join(__dirname, "dist")));

  // ─── Root redirect ────────────────────────────────────────────────
  app.get("/", (_req: Request, res: Response) => {
    res.redirect("/chat.html");
  });

  // ─── JSON body parser (for API/MCP routes only) ───────────────────
  app.use(express.json());

  // ─── Chat API routes ──────────────────────────────────────────────
  registerChatRoutes(app);

  // ─── MCP protocol endpoint ────────────────────────────────────────
  app.all("/mcp", async (req: Request, res: Response) => {
    const server = factory();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    res.on("close", () => {
      transport.close().catch(() => {});
      server.close().catch(() => {});
    });
    try {
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    } catch (error) {
      console.error("MCP error:", error);
      if (!res.headersSent) {
        res.status(500).json({
          jsonrpc: "2.0",
          error: { code: -32603, message: "Internal server error" },
          id: null,
        });
      }
    }
  });

  const httpServer = app.listen(port, () => {
    console.log(`Parity Explorer`);
    console.log(`  Chat:  http://localhost:${port}/`);
    console.log(`  MCP:   http://localhost:${port}/mcp`);
  });

  const shutdown = () => {
    console.log("\nShutting down...");
    httpServer.close(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

async function startStdioServer(factory: () => McpServer): Promise<void> {
  const server = factory();
  await server.connect(new StdioServerTransport());
}

async function main() {
  if (process.argv.includes("--stdio")) {
    await startStdioServer(createServer);
  } else {
    await startStreamableHTTPServer(createServer);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
