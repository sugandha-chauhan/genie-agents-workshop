# Employee Turnover — Genie Ontology

## Domain

Employee Turnover — rolling 12-month (R12) and 3-month annualized (R3MA) turnover rates across operating units, job families, departments, and shifts.

## Data Architecture

```
fact_workforce_metrics (source) → gold_turnover_all (gold view) → turnover_metrics (metric view) → Genie Space
```

- **Source table:** `fact_workforce_metrics` — one row per employee × month × metric type. Contains `numerator_num`, `denominator_num`, `max_r12_month_num`, and `metric_nm` (which distinguishes 4 turnover rate calculations).
- **Gold view:** `gold_turnover_all` — passthrough of all 73 columns (single-table domain, no joins needed).
- **Metric view:** `turnover_metrics` — the **sole** data source for the Genie space. No raw tables exposed.

## Metric View: `turnover_metrics`

### 14 Dimensions

| Dimension | Expression | Display Name | Synonyms |
|---|---|---|---|
| `op_unit` | `op_unit_name` | Operating Unit | hospital, facility, site, campus, op unit |
| `business_unit` | `business_unit` | Business Unit | — |
| `region` | `region` | Region | — |
| `department_org` | `department_org` | Department Organization | org |
| `department_sub_org` | `department_sub_org` | Department Sub-Organization | sub org, sub organization |
| `department_type` | `department_type` | Department Type | — |
| `cost_center` | `cost_center_name` | Cost Center / Department | department, dept, cost center |
| `job_family_grouper` | `job_Family_grouper` | Job Family Grouper | JFG, job family group, job group |
| `job_family` | `job_family_name` | Job Family | — |
| `job_code_grouper` | `job_code_grouper` | Job Code Grouper | — |
| `shift` | `shift` | Shift | work shift, job shift |
| `month` | `month_dts` | Month | month date, reporting month, metric month |
| `department_geography` | `department_geography` | Department Geography | — |
| `department_site` | `department_site` | Department Site | — |

### 5 Measures

All turnover measures use the pattern:
```
SUM(numerator) / (SUM(denominator) / MAX(max_r12_months))
```
with `FILTER (WHERE metric_nm = '...')` to select the correct metric type.

| Measure | Display Name | Metric Filter | Synonyms | Format |
|---|---|---|---|---|
| `overall_turnover_r12` | Overall Turnover Rate (R12) | `Rolling 12 Month Turnover Rate` | turnover rate, overall turnover, R12, annual turnover | % (2 dp) |
| `first_year_turnover_r12` | First Year Turnover (R12) | `Rolling 12 Month First Year Employee Org Turnover` | first year turnover, new hire turnover, R12 first year | % (2 dp) |
| `overall_turnover_r3` | Overall Turnover (R3 Annualized) | `3 Month Annualized Overall Turnover Rate` | 3 month overall, R3MA, annualized | % (2 dp) |
| `first_year_turnover_r3` | First Year Turnover (R3 Annualized) | `3 Month Annualized First Year Turnover Rate` | 3 month first year, R3 first year | % (2 dp) |
| `headcount` | Employee Headcount | (COUNT DISTINCT emplid) | employees, head count, FTE count | number (0 dp) |

## Genie Space Configuration

### Data Source

**1 metric view only:** `turnover_metrics`

### 3 Example SQL Queries

#### E1: Basic — Overall Turnover by OP Unit (R12)
```sql
SELECT op_unit,
       MEASURE(overall_turnover_r12) AS turnover_rate
FROM   turnover_metrics
WHERE  month = '2026-07-01'
GROUP  BY ALL
ORDER  BY turnover_rate DESC
```

#### E2: Hard — NMH Dept with JFG + Headcount Filter
```sql
SELECT cost_center AS department,
       MEASURE(overall_turnover_r12) AS turnover_rate,
       MEASURE(headcount) AS employees
FROM   turnover_metrics
WHERE  month = '2026-07-01'
  AND  op_unit = 'NMH'
  AND  job_family_grouper = 'Patient Care Support'
GROUP  BY ALL
HAVING MEASURE(headcount) >= 10
ORDER  BY turnover_rate DESC
```

#### E3: R3MA with Sub-Org Filter
```sql
SELECT op_unit,
       MEASURE(overall_turnover_r3) AS turnover_rate
FROM   turnover_metrics
WHERE  month = '2026-07-01'
  AND  department_sub_org IN ('NHN Hospital', 'AMC Hospital')
GROUP  BY ALL
ORDER  BY turnover_rate DESC
```

### 5 Text Instructions

| ID | Instruction |
|---|---|
| I1 | When the user asks about "turnover" or "turnover rate" without specifying a time window, use `overall_turnover_r12` (rolling 12 month). |
| I2 | When the user mentions "3 month", "3-month", "annualized", "R3", or "R3MA", use the R3 measures (`overall_turnover_r3` or `first_year_turnover_r3`). |
| I3 | When the user mentions "first year", "new hire", or "first-year" turnover, use the first year turnover measures. Combined with "3 month" → `first_year_turnover_r3`; otherwise → `first_year_turnover_r12`. |
| I4 | Do not filter by `metric_nm`, `numerator_num`, or `denominator_num`. The measures already apply the correct metric filters internally. |
| I5 | When the user asks about departments, cost centers, or job families "with N or more employees", use `HAVING MEASURE(headcount) >= N` to filter low-volume groups. |

### 1 Filter

| Name | Expression |
|---|---|
| NHN + AMC Hospitals | `department_sub_org IN ('NHN Hospital', 'AMC Hospital')` |

### 5 Starter Questions

1. Which operating unit has the highest overall turnover rate?
2. What is the first year turnover rate by job family grouper?
3. Which department at NMH with 10+ employees has the highest turnover?
4. How does 3-month annualized turnover compare across operating units for NHN and AMC hospitals?
5. Which shift has the highest overall turnover rate?

## Benchmark Questions (20)

### R12 Set (Q1–Q10)

All queries use `WHERE month = '2026-07-01'`.

| Q | Question | Dimension |
|---|---|---|
| Q1 | Which OP unit has the highest overall turnover? | `op_unit` |
| Q2 | Which OP unit has the lowest overall turnover? | `op_unit` |
| Q3 | Which OP unit has the highest first year turnover? | `op_unit` |
| Q4 | Which OP unit has the lowest first year turnover? | `op_unit` |
| Q5 | Which JFG has the highest overall turnover? | `job_family_grouper` |
| Q6 | Which JFG has the lowest overall turnover? | `job_family_grouper` |
| Q7 | Which JFG has the highest first year turnover? | `job_family_grouper` |
| Q8 | Which JFG has the lowest first year turnover? | `job_family_grouper` |
| Q9 | Which NMH dept with JFG=Patient Care Support and 10+ employees has highest overall turnover? | `cost_center` (composite) |
| Q10 | Which shift has the highest overall turnover? | `shift` |

### R3MA Set (Q1–Q10)

All queries add `AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')` to the R12 filter. Same question structure but uses R3 measures.
