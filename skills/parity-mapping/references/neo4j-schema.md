# Neo4j Schema Reference

## Node Labels

| Label | Description |
|-------|-------------|
| `Report` | MicroStrategy report (container for metrics/attributes) |
| `Metric` | MSTR metric (has formula, aggregation logic) |
| `Attribute` | MSTR attribute (dimension column) |
| `DerivedMetric` | MSTR derived metric (DAX-like composition of other metrics) |
| `Filter` | MSTR report filter |
| `Prompt` | MSTR report prompt |

## Key Properties

### Metric / Attribute nodes
- `guid` — 32-character hex identifier
- `name` — display name
- `formula` — MSTR formula expression (metrics only)
- `parity_status` / `updated_parity_status` — migration status (Complete, Planned, Drop, Not Planned)
- `pb_semantic_name` / `updated_pb_semantic_name` — direct PBI mapping (S1 signal source)
- `pb_semantic_model` / `updated_pb_semantic_model` — PBI model name
- `ade_db_table` — ADE warehouse table (NOT `ade_table`)
- `ade_db_column` — ADE warehouse column (S2 signal source)
- `inherited_priority_level` — P1/P2/P3 priority from report inheritance

### COALESCE Pattern

Always prefer updated fields over originals:
```cypher
COALESCE(m.updated_pb_semantic_model, m.pb_semantic_model) AS pb_model
COALESCE(m.updated_parity_status, m.parity_status) AS parity_status
COALESCE(m.updated_pb_semantic_name, m.pb_semantic_name) AS pb_name
```

## Relationships

| Relationship | From | To | Description |
|-------------|------|-----|-------------|
| `DEPENDS_ON` | Metric | Metric/Attribute | Metric depends on another object |
| `BELONGS_TO` | Metric/Attribute | Report | Object belongs to a report |
| `FLOW` | various | various | Data flow relationship |

**Important**: Use `DEPENDS_ON`, NOT `:USES`. The `:USES` relationship does not exist in this schema.

## Example Queries

### All prioritised metrics for a report
```cypher
MATCH (r:Report {guid: $reportGuid})<-[:BELONGS_TO]-(m:Metric)
WHERE m.inherited_priority_level IS NOT NULL
RETURN m.guid, m.name, m.formula,
       COALESCE(m.updated_parity_status, m.parity_status) AS parity_status,
       COALESCE(m.updated_pb_semantic_name, m.pb_semantic_name) AS pb_name
```

### Metric lineage
```cypher
MATCH (m:Metric {guid: $metricGuid})-[:DEPENDS_ON*1..3]->(dep)
RETURN dep.guid, dep.name, labels(dep)[0] AS type
```
