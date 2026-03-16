/**
 * Tool names that produce UI-renderable results.
 * Used by the chat frontend to decide which tool results get iframes.
 */
export const UI_TOOLS = new Set([
  "search-reports",
  "run-parity-analysis",
  "get-mapping-detail",
  "get-parity-summary",
]);
