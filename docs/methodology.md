# Genie Agent Workshop — 9-Workstream Methodology

A structured framework for building production-quality Databricks Genie Agents. Each workstream builds on the previous, producing a tested, governed, and trustworthy Genie space.

## The 9 Workstreams

### Workstream 0: Domains & Discovery

**Goal:** Scope the Genie space to one business domain.

A Genie space works best when scoped to **one business domain** with a clear set of questions and a well-understood data model. Think of it as a subject-matter expert who knows everything about one topic.

**Checklist:**
- [ ] What questions will users ask? (define benchmark questions)
- [ ] Which tables answer those questions?
- [ ] Can we reduce to 1–2 gold views?
- [ ] Who are the users and what access do they need?

### Workstream 1: Data Preparation

**Goal:** Create denormalized gold views, UC metric views, and UC metadata.

Genie performs best with **denormalized, pre-joined gold views** rather than raw normalized tables.

**Pattern:**
```
Raw sources → Bronze → Silver → Gold View (denormalized) → Genie Space
```

**Best practices:**
- One wide table per domain (minimize joins)
- Give every column a human-readable `COMMENT`
- Use `ALTER TABLE ... SET TAGS` for primary/foreign key hints
- Pre-compute commonly requested metrics in the view

### Workstream 1.5: UC Metric Views

**Goal:** Define reusable business metrics once, use everywhere.

A **metric view** separates measure definitions from dimension groupings. Consumers use `MEASURE()` syntax:

```sql
SELECT hospital,
       MEASURE(discharges)   AS discharges,
       MEASURE(avg_los_days) AS avg_los_days
FROM   catalog.schema.los_metrics
WHERE  fiscal_year = 2026
GROUP  BY ALL;
```

**Metric view vs. SQL expressions:**

| Feature | Metric View | Genie SQL Expression |
|---|---|---|
| Scope | Workspace-wide (UC) | Single Genie space |
| Reusable across dashboards | Yes | No |
| Synonyms | Built-in YAML | Configured in space |
| Formats (currency, %) | Built-in YAML | Manual |

**Design decisions:**
- Use **explicit dimension list** (not wildcard `*`) to control what the agent can group by
- Include **synonyms** in YAML so Genie maps business terms to columns
- Include **format** specs so dashboards auto-format

### Workstream 2: SQL Expressions

**Goal:** Register pre-built filters, measures, and fields with synonyms.

**Three types:**

| Type | Purpose | Example |
|---|---|---|
| Measures | Pre-defined aggregations | `AVG(los_observed_days)` → "average length of stay" |
| Filters | Scoped subsets | `fiscal_year = 2026` → "FY2026" |
| Fields with Synonyms | Alias columns | `hospital` → "site", "facility" |

**Key principle:** SQL expressions > text instructions. Expressions are unambiguous, testable, and reproducible.

### Workstream 3: Example SQL Queries

**Goal:** Curate static + parameterized queries as trusted assets.

Example queries are the **most powerful tuning lever** for a Genie space. When a user's question matches an example, Genie uses the curated SQL rather than generating its own.

**Best practices:**
- Start with benchmark questions from business users
- Write SQL that produces the validated answer
- Register as trusted assets in the Genie space
- **2–3 carefully chosen examples beat 20 exhaustive ones** — Genie generalizes from patterns

**Example query categories:**
1. **Basic** — single dimension, single measure (teaches the pattern)
2. **Hard** — composite filters + HAVING clause (teaches complex queries)
3. **Variant** — different measure family or filter scope (teaches measure routing)

### Workstream 4: Text Instructions

**Goal:** Add minimal, specific domain rules.

Text instructions are the **last resort**, not the first. If you're writing a paragraph, you probably need a better SQL expression or example query.

**When to use:**
- Domain rules that can't be expressed in SQL (e.g., "fiscal year starts in September")
- Disambiguation (e.g., "LOS means length of stay, not the city")
- Default behavior (e.g., "default to rolling 12 month")
- Column preference (e.g., "use `los_observed_days` unless user specifies IP or OBSV")

**When NOT to use:**
- Metric definitions → use SQL expressions
- Join logic → use a denormalized gold view
- Common query patterns → use example queries

**Limit:** 5 instructions maximum. Each should be one sentence.

### Workstream 5: AI/BI Dashboard

**Goal:** Wire curated metrics into governed dashboards.

Dashboards are the visual layer on top of the Genie space:
- Both use the same gold view as their data source
- Dashboard widgets mirror the same metrics from SQL expressions
- Users can click "Ask Genie" from a dashboard to drill deeper

### Workstream 6: Benchmarks & Monitoring

**Goal:** Build a benchmark suite + weekly digest feedback loop.

A Genie space is **never done**. The benchmark workflow:

```
1. Define benchmark questions (from business users)
2. Record expected answers (from validated dashboards/reports)
3. Ask Genie each question and compare to expected answer
4. Score: correct, partially correct, or wrong
5. Fix: add/refine SQL expressions, example queries, or text instructions
6. Re-run benchmarks
7. Repeat weekly with user feedback
```

### Workstream 7: Data Access Model

**Goal:** Configure security, row/column controls, and space permissions.

Genie inherits Unity Catalog’s security model:

| Layer | Controls | Managed By |
|---|---|---|
| Unity Catalog | Who can SELECT from the gold view | Data Engineer (GRANT/REVOKE) |
| Row-Level Security | Which rows a user sees (e.g., only their hospital) | Data Engineer (row filters) |
| Genie Space Permissions | Who can access the space | Space Admin |

### Workstream 8: Genie One

**Goal:** Surface approved spaces to end users.

Genie One is the single entry point where end users discover and access approved Genie spaces. Admins curate which spaces appear; permissions are inherited.

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Genie data source | Metric view only | Reduces ambiguity; measures are pre-defined |
| Dimension list | Explicit (not `*`) | Controls what the agent can group by |
| Example query count | 2–3 per domain | Quality over quantity; Genie generalizes |
| Text instructions | 5 max, one sentence each | Last resort — SQL expressions preferred |
| Default time filter | None (dynamic) | Let users ask naturally; add instruction for "most recent month" |
| Gold view design | Denormalized single view | Fewer joins = less AI ambiguity |
| Measure formulas | In metric view YAML | Single source of truth across Genie + dashboards |

## Workshop Format

| | Duration | Activity |
|---|---|---|
| **Day 1** | 2 hrs | Enablement — concept walkthrough (workstreams 0–8) |
| | 2 hrs | Hands-on build — workstreams 1–4 on your data |
| **Day 2** | 1 hr | Refresher — recap, Q&A, set build targets |
| | Build | Hands-on — continue workstreams 5–8 |
| | 30 min | Present your Genie space — demo & peer review |
