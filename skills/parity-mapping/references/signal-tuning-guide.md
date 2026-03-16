# Signal Tuning Guide

## Current Weights

| Signal | Weight | Rationale |
|--------|--------|-----------|
| S2 — Column Lineage | 0.30 | Strong physical evidence but not always available |
| S3 — Name Similarity | 0.35 | Most broadly applicable, fires for nearly every object |
| S4 — Formula Analysis | 0.25 | Structural comparison, metrics only |
| S5 — Table Context | 0.10 | Supplementary, reinforces other signals |

Weights are defined in `config.py` as `WEIGHT_S2_LINEAGE`, `WEIGHT_S3_NAME`, `WEIGHT_S4_FORMULA`, `WEIGHT_S5_CONTEXT`.

## Confidence Thresholds

| Level | Threshold | Config Key |
|-------|-----------|------------|
| Confirmed | >= 0.90 | `THRESHOLD_CONFIRMED` |
| High | >= 0.70 | `THRESHOLD_HIGH` |
| Medium | >= 0.50 | `THRESHOLD_MEDIUM` |
| Low | >= 0.30 | `THRESHOLD_LOW` |
| Unmapped | < 0.30 | (everything below Low) |

## When to Adjust Weights

### Increase S2 (Column Lineage) weight
- When: ADE lineage metadata is comprehensive and well-maintained
- Effect: More objects classified as High confidence via lineage alone
- Risk: If lineage metadata is stale, may produce false high-confidence matches

### Increase S3 (Name Similarity) weight
- When: MSTR and PBI naming conventions are closely aligned
- Effect: More objects get matched, but more false positives possible
- Risk: Homonyms across different domains may match incorrectly

### Increase S4 (Formula Analysis) weight
- When: MSTR formulas are well-structured and PBI DAX follows similar patterns
- Effect: Better differentiation between metrics with similar names but different calculations
- Risk: Only applies to metrics, no effect on attributes

### Lower THRESHOLD_MEDIUM from 0.50 to 0.40
- Effect: More objects classified as Medium instead of Low
- Use case: When you want fewer "manual verification required" items in the report
- Risk: Some genuinely weak matches may be promoted to Medium

## Multi-Signal Bonus

When 2+ signals fire for the same candidate, a +0.10 bonus is added per extra signal.

Example: S3 alone = 0.55 (Medium). S3 + S5 together = 0.55 + 0.10 = 0.65 (still Medium but closer to High). S3 + S4 + S5 = 0.55 + 0.20 = 0.75 (High).

The bonus incentivises convergent evidence from multiple independent signals.

## The 0.99 Cap

No combination of S2-S5 signals can exceed 0.99. Only S1 (direct Neo4j mapping) produces a 1.0 score. This preserves the distinction between algorithmically matched objects and manually verified ones.

## Testing Weight Changes

After modifying weights in `config.py`:
1. Run the full dataset analysis
2. Compare confidence distributions with previous run
3. Pay attention to objects that changed classification level
4. Check if the changes align with domain expert expectations
