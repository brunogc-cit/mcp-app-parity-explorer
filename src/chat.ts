/**
 * Client-side chat logic — Parity Explorer Chat Interface
 *
 * Sends user messages to /api/chat, reads SSE stream,
 * renders text as bubbles and tool results as embedded iframes.
 */

import "./chat-styles.css";

// ═══════════════════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════════════════

const messages: Array<{ role: string; content: string }> = [];
let isStreaming = false;

// ═══════════════════════════════════════════════════════════════════════════
// DOM refs
// ═══════════════════════════════════════════════════════════════════════════

const chatMessages = document.getElementById("chat-messages")!;
const chatInput = document.getElementById("chat-input") as HTMLTextAreaElement;
const sendBtn = document.getElementById("send-btn")!;
const statusDot = document.getElementById("status-dot")!;
const statusText = document.getElementById("status-text")!;

// ═══════════════════════════════════════════════════════════════════════════
// Event handlers
// ═══════════════════════════════════════════════════════════════════════════

sendBtn.addEventListener("click", sendMessage);
chatInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});

// Auto-resize textarea
chatInput.addEventListener("input", () => {
  chatInput.style.height = "auto";
  chatInput.style.height = Math.min(chatInput.scrollHeight, 150) + "px";
});

// Suggestion buttons
document.querySelectorAll(".suggestion").forEach((btn) => {
  btn.addEventListener("click", () => {
    const msg = (btn as HTMLElement).dataset.msg!;
    chatInput.value = msg;
    sendMessage();
  });
});

// Listen for tool requests from iframes
window.addEventListener("message", async (event) => {
  if (event.data?.type === "tool-request") {
    const { name, arguments: args } = event.data;
    try {
      const res = await fetch("/api/tool", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, arguments: args }),
      });
      const result = await res.json();

      // Find the iframe that sent the request and inject the result
      const iframes = document.querySelectorAll<HTMLIFrameElement>(".tool-iframe");
      for (const iframe of iframes) {
        if (iframe.contentWindow === event.source) {
          iframe.contentWindow?.postMessage({
            type: "inject-tool-result",
            data: result.data,
          }, "*");
          break;
        }
      }
    } catch (err) {
      console.error("Tool request failed:", err);
    }
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// Send message
// ═══════════════════════════════════════════════════════════════════════════

async function sendMessage(): Promise<void> {
  const text = chatInput.value.trim();
  if (!text || isStreaming) return;

  // Remove welcome message
  const welcome = chatMessages.querySelector(".welcome-message");
  if (welcome) welcome.remove();

  // Add user message
  messages.push({ role: "user", content: text });
  appendUserBubble(text);
  chatInput.value = "";
  chatInput.style.height = "auto";

  // Start streaming
  isStreaming = true;
  setStatus("thinking", "Thinking...");

  const assistantBubble = createAssistantBubble();
  const textContainer = assistantBubble.querySelector(".bubble-text")!;
  let fullText = "";

  try {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ messages }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop()!; // Keep incomplete line in buffer

      for (const line of lines) {
        if (line.startsWith("event: ")) {
          continue; // Event type line — data follows on next line
        }
        if (line.startsWith("data: ")) {
          const rawData = line.slice(6);
          try {
            const data = JSON.parse(rawData);
            // Need to determine event type — parse from the preceding event line
            handleSSEData(data, textContainer, assistantBubble);
          } catch {
            // Skip malformed data
          }
        }
      }
    }

    // Finalize assistant message
    if (fullText) {
      messages.push({ role: "assistant", content: fullText });
    }
  } catch (err) {
    appendErrorBubble(String(err));
  } finally {
    isStreaming = false;
    setStatus("ready", "Ready");
    scrollToBottom();
  }

  // Inner handler for SSE data
  function handleSSEData(
    data: Record<string, unknown>,
    container: Element,
    bubble: Element,
  ): void {
    // Delta — streaming text
    if ("text" in data && typeof data.text === "string") {
      setStatus("streaming", "Responding...");
      fullText += data.text;
      container.innerHTML = formatMarkdown(fullText);
      scrollToBottom();
      return;
    }

    // Tool call notification
    if ("name" in data && "id" in data && "input" in data) {
      setStatus("tool", `Calling ${data.name}...`);
      const toolNotice = document.createElement("div");
      toolNotice.className = "tool-notice";
      toolNotice.innerHTML = `<span class="tool-icon">&#9881;</span> Calling <strong>${escapeHtml(data.name as string)}</strong>`;
      bubble.insertBefore(toolNotice, bubble.querySelector(".bubble-text"));
      scrollToBottom();
      return;
    }

    // Tool result with UI
    if ("data" in data && data.hasUi) {
      const toolData = data.data as Record<string, unknown>;
      const iframe = createToolIframe(toolData);
      bubble.insertBefore(iframe, bubble.querySelector(".bubble-text"));
      scrollToBottom();
      return;
    }

    // Done
    if ("cost" in data || "duration" in data) {
      const cost = data.cost as number;
      const duration = data.duration as number;
      if (cost || duration) {
        const meta = document.createElement("div");
        meta.className = "message-meta";
        const parts: string[] = [];
        if (duration) parts.push(`${(duration / 1000).toFixed(1)}s`);
        if (cost) parts.push(`$${cost.toFixed(4)}`);
        meta.textContent = parts.join(" · ");
        bubble.appendChild(meta);
      }
      return;
    }

    // Error
    if ("message" in data && !("text" in data)) {
      appendErrorBubble(data.message as string);
      return;
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// DOM helpers
// ═══════════════════════════════════════════════════════════════════════════

function appendUserBubble(text: string): void {
  const div = document.createElement("div");
  div.className = "message user";
  div.innerHTML = `<div class="bubble">${escapeHtml(text)}</div>`;
  chatMessages.appendChild(div);
  scrollToBottom();
}

function createAssistantBubble(): HTMLElement {
  const div = document.createElement("div");
  div.className = "message assistant";
  div.innerHTML = `<div class="bubble"><div class="bubble-text"><span class="typing-indicator">●●●</span></div></div>`;
  chatMessages.appendChild(div);
  scrollToBottom();
  return div.querySelector(".bubble")!;
}

function appendErrorBubble(error: string): void {
  const div = document.createElement("div");
  div.className = "message error";
  div.innerHTML = `<div class="bubble">Error: ${escapeHtml(error)}</div>`;
  chatMessages.appendChild(div);
  scrollToBottom();
}

function createToolIframe(data: Record<string, unknown>): HTMLElement {
  const wrapper = document.createElement("div");
  wrapper.className = "tool-iframe-wrapper";

  // Determine height based on action type
  const action = data.action as string;
  let height = "500px";
  if (action === "search-results") height = "300px";
  if (action === "detail") height = "450px";
  if (action === "summary") height = "350px";

  const iframe = document.createElement("iframe");
  iframe.className = "tool-iframe";
  iframe.src = "/mcp-app.html";
  iframe.style.height = height;
  iframe.sandbox.add("allow-scripts", "allow-same-origin");
  iframe.setAttribute("loading", "lazy");

  // Inject data after iframe loads
  iframe.addEventListener("load", () => {
    setTimeout(() => {
      iframe.contentWindow?.postMessage({
        type: "inject-tool-result",
        data: data,
      }, "*");
    }, 500); // Wait for app.ts to initialize
  });

  // Collapse/expand toggle
  const header = document.createElement("div");
  header.className = "iframe-header";
  const actionLabel = {
    "search-results": "Search Results",
    "parity-analysis": "Parity Analysis Dashboard",
    "detail": "Signal Breakdown",
    "summary": "Parity Summary",
  }[action] || "Tool Result";
  header.innerHTML = `<span>${actionLabel}</span><button class="collapse-btn" title="Toggle">▼</button>`;
  header.querySelector(".collapse-btn")!.addEventListener("click", () => {
    wrapper.classList.toggle("collapsed");
  });

  wrapper.appendChild(header);
  wrapper.appendChild(iframe);
  return wrapper;
}

function scrollToBottom(): void {
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function setStatus(state: string, text: string): void {
  statusDot.className = `status-dot ${state}`;
  statusText.textContent = text;
}

function formatMarkdown(text: string): string {
  // Basic markdown rendering for chat
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.*?)\*/g, "<em>$1</em>")
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\n/g, "<br>");
}

function escapeHtml(str: string): string {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
