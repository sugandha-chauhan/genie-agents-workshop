# Genie Agents Workshop

A reusable framework for building production-quality **Databricks Genie Agents** on healthcare workforce and operational data. Two fully worked domains demonstrate the complete 9-workstream methodology for creating trusted, governed Genie spaces.

## Domains

| Domain | Notebook | Metric View | Measures | Dimensions | Benchmarks |
|---|---|---|---|---|---|
| **Employee Turnover** | `Employee Turnover Genie Workshop` | `turnover_metrics` | 5 | 14 | 20 questions |
| **Length of Stay** | `Length of Stay Genie Workshop` | `los_metrics` | 8 | 10 | 14 questions |

## Quick Start

1. **Open** either workshop notebook in Databricks
2. **Update** `CATALOG` and `SCHEMA` in the first code cell to match your environment
3. **Run cells sequentially** — sample data cells can be skipped if you have your own tables
4. **Follow the workstreams** — each section builds on the previous

### Environment Pattern

Both notebooks use the same config-once pattern:

```python
CATALOG = "your_catalog"         # <-- Update based on your access
SCHEMA  = "your_schema"           # <-- Update based on your access
dbutils.widgets.text("catalog", CATALOG, "Catalog")
dbutils.widgets.text("schema_name", SCHEMA, "Schema")
```

- **Python cells** reference `CATALOG` and `SCHEMA` variables directly
- **SQL cells** use `${catalog}.${schema_name}` widget substitution
- Change once at the top → entire notebook follows

## Repository Structure

```
genie-agents-workshop/
├── README.md                                    # This file
├── Employee Turnover Genie Workshop.py          # Turnover notebook (25 cells)
├── Length of Stay Genie Workshop.py              # LOS notebook (43 cells)
└── docs/
    ├── methodology.md                           # 9-workstream framework
    ├── ontology_turnover.md                     # Turnover Genie ontology
    └── ontology_los.md                          # LOS Genie ontology
```

## Architecture

Both domains follow the same layered architecture:

```
Source Tables → Gold View (denormalized) → Metric View (MEASURE syntax) → Genie Space
```

### Turnover
- **Source:** `fact_workforce_metrics` (single table — numerator/denominator per employee × month × metric type)
- **Gold view:** `gold_turnover_all` (passthrough — single-table domain)
- **Metric view:** `turnover_metrics` (14 dimensions, 5 measures)
- **Genie space:** Metric-view-only design — no raw tables exposed to the agent

### Length of Stay
- **Sources:** `rpt_vizient_and_hd_unioned` (fact, 35K encounters) + `dim_date` (fiscal calendar) + `rpt_criticalcare_domainsummary` (ICU) + 3 reference dimensions
- **Gold view:** `gold_los` (6-table join, population: `har_in_hd_dm_flag = 1`)
- **Metric view:** `los_metrics` (10 dimensions, 8 measures)

## Genie Space Configuration

Each domain documents the complete Genie space ontology in `docs/`:

- **Data sources** — which metric view(s) to register
- **Example SQL queries** — curated queries that teach the agent patterns (most powerful tuning lever)
- **Text instructions** — minimal domain rules the agent can't infer from SQL
- **Filters** — pre-built population scopes users can toggle
- **Starter questions** — seed questions for the chat interface
- **Synonyms** — business terms mapped to column names

See `docs/ontology_turnover.md` and `docs/ontology_los.md` for the full specifications.

## Methodology

The 9-workstream framework is documented in `docs/methodology.md`. Key principles:

1. **SQL expressions > text instructions** — teach by showing, not telling
2. **Metric-view-only Genie spaces** — no raw tables exposed to the agent
3. **Explicit dimensions** in metric view YAML — no wildcard `*`
4. **2-3 carefully chosen example queries** beat 20 exhaustive ones
5. **Benchmark-driven iteration** — define questions → score Genie → fix → repeat weekly

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Genie data source | Metric view only | Reduces ambiguity; measures are pre-defined |
| Dimension list | Explicit (not `*`) | Controls what the agent can group by |
| Example query count | 2-3 per domain | Quality over quantity; Genie generalizes |
| Text instructions | 5 max, specific | Last resort — SQL expressions are preferred |
| Default time filter | None (dynamic) | Let users ask naturally; add instruction for "most recent month" |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A catalog and schema where you have `CREATE TABLE` / `CREATE VIEW` permissions
- Serverless SQL warehouse (for Genie space) or any warehouse for notebook execution
- Databricks SDK (`databricks-sdk`) pre-installed on serverless compute
