# Sample Output Formats

## Markdown Report (Full / Single Report)

```markdown
# MSTR → Power BI Migration Mapping Report
> Generated: 2026-03-06 17:37 UTC
> Scope: **Prioritized objects only**

## Executive Summary
| Category | Total | Dropped | In Scope | Mapped | Unmapped | Coverage |
|----------|-------|---------|----------|--------|----------|----------|
| Metrics  | 3     | 0       | 3        | 3      | 0        | 100.0%   |
| Attributes | 16  | 0       | 16       | 12     | 4        | 75.0%    |
| **Total** | **19** | **0** | **19**   | **15** | **4**    | **78.9%** |

## Confidence Distribution
| Level     | Metrics | Attributes | Total | % of Mapped |
|-----------|---------|------------|-------|-------------|
| Confirmed | 1       | 5          | 6     | 40.0%       |
| High      | 1       | 0          | 1     | 6.7%        |
| Medium    | 1       | 1          | 2     | 13.3%       |
| Low       | 0       | 6          | 6     | 40.0%       |

## Metrics — Confirmed (1)
| MSTR Name | PBI Name | PBI Model | Confidence | Signals | Priority | Status |
|-----------|----------|-----------|------------|---------|----------|--------|
| Billed Sales Value | Billed Sales Value | Sales | 100% | S1 | P1 | Complete |

## Attributes — Low (6)
| MSTR Name | PBI Name | PBI Model | ADE Column | Confidence | Signals |
|-----------|----------|-----------|------------|------------|---------|
| Shipping Method | PO Shipping Method | Purchase | — | 46% | S3 |
```

## HTML Dashboard (Batch Reports)

The batch report produces a styled HTML file with:
- Hero section with metadata (timestamp, report count)
- 5-column KPI grid: Reports, Total Objects, Coverage %, Avg Confidence, C/H/M/L distribution
- Per-report summary table with coverage bars and confidence badges
- Drill-down cards per report with individual metrics/attributes tables
- Colour-coded badges: Confirmed (green), High (blue), Medium (orange), Low (red)
