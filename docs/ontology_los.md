# Length of Stay (LOS) — Genie Ontology

## Domain

Length of Stay (LOS) — hospital discharge volumes, average LOS, Vizient risk-adjusted metrics, ICU rates, and payer analysis across 8 hospital sites.

## Data Architecture

```
6 source tables → gold_los (denormalized join) → los_metrics (metric view) → Genie Space
```

### Source Tables

| Table | Purpose | Rows (sample) |
|---|---|---|
| `rpt_vizient_and_hd_unioned` | Main fact — all hospital discharges with LOS, Vizient, demographics | 35,000 |
| `dim_date` | Fiscal calendar (FY starts Sep 1) | 1,461 |
| `rpt_criticalcare_domainsummary` | ICU visits, ventilator, critical care flags | 3,680 |
| `ref_fytd_through` | Fiscal year-to-date completeness flags | 36 |
| `ref_vizient_top_decile_oe` | Vizient O/E benchmarks by FY × hospital | 24 |
| `ref_physician_employment` | Provider NM-employed status | ~200 |

### Gold View: `gold_los`

- **Joins** all 6 source tables into one denormalized view
- **Population filter:** `har_in_hd_dm_flag = 1` (All Hospital Discharges IP & OBSV)
- **Key columns:** hospital, fiscal_year, fiscal_month, account_class_name (IP/OBSV), discharge_department_name, payer_category, los_observed_days, los_inpatient_days, los_observation_hours, long_los_flag, case_mix_index, icu_encounter_flag, mortality_flag
- **Hospital sort order:** NMH, CDH, PH, MCH, LFH, KH, VH, DHG

### Metric View: `los_metrics`

Sole data source for the Genie space.

## Metric View: `los_metrics`

### 10 Dimensions

| Dimension | Expression | Display Name | Synonyms |
|---|---|---|---|
| `hospital` | `hospital` | Hospital | site, facility, op unit, governed op unit |
| `fiscal_year` | `fiscal_year` | Fiscal Year | FY, year |
| `fiscal_month` | `fiscal_month` | Fiscal Month | — |
| `month_name` | `month_name` | Month Name | — |
| `account_class` | `account_class_name` | Account Class | encounter type, patient class, IP or OBSV |
| `department` | `discharge_department_name` | Discharge Department | unit, ward, discharge unit |
| `payer` | `payer_category` | Payer Category | financial class, insurance, coverage |
| `discharge_date` | `discharge_date` | Discharge Date | — |
| `vizient_service_line` | `vizient_service_line` | Vizient Service Line | — |
| `primary_diagnosis` | `primary_dx_name` | Primary Diagnosis | diagnosis, dx |

### 8 Measures

| Measure | Expression | Display Name | Synonyms | Format |
|---|---|---|---|---|
| `discharges` | `COUNT(hsp_account_id)` | Discharges | discharge count, volume, encounters | number (0 dp) |
| `avg_los_days` | `ROUND(AVG(los_observed_days), 2)` | Average LOS (Days) | average length of stay, mean LOS, avg LOS | number (2 dp) |
| `avg_inpatient_los_days` | `ROUND(AVG(los_inpatient_days), 2)` | Average Inpatient LOS (Days) | inpatient LOS, IP LOS | number (2 dp) |
| `avg_observation_los_hours` | `ROUND(AVG(CASE WHEN account_class_name = 'OBSV' THEN los_observation_hours END), 2)` | Average Observation LOS (Hours) | observation LOS, OBSV LOS | number (2 dp) |
| `long_los_rate` | `ROUND(AVG(long_los_flag) * 100, 1)` | Long LOS Rate (%) | long stay rate, extended stay rate | percentage (1 dp) |
| `avg_case_mix_index` | `ROUND(AVG(case_mix_index), 4)` | Average CMI | CMI, case mix | number (4 dp) |
| `icu_encounter_rate` | `ROUND(AVG(icu_encounter_flag) * 100, 1)` | ICU Encounter Rate (%) | ICU rate, critical care rate | percentage (1 dp) |
| `mortality_rate` | `ROUND(AVG(mortality_flag) * 100, 2)` | Mortality Rate (%) | in-hospital mortality | percentage (2 dp) |

## Key Domain Rules

1. **Fiscal calendar:** FY starts September 1. FM 1 = Sep, FM 9 = May, FM 12 = Aug.
2. **Population:** `har_in_hd_dm_flag = 1` (All Hospital Discharges IP & OBSV)
3. **Hospital sort order:** NMH, CDH, PH, MCH, LFH, KH, VH, DHG (by `hospital_sort`)
4. **LOS metric selection:**
   - Overall IP & OBSV → `los_observed_days` (fractional days)
   - Inpatient portion only → `los_inpatient_days`
   - Observation portion → `los_observation_hours` (hours, not days)
5. **Rounding:** `ROUND(AVG(...), 2)` for all LOS averages
6. **Vizient-qualified subset:** Use `vizient_los_denom_flag = 1` for Vizient-specific analyses

## Genie Space Configuration

### Data Source

**1 metric view:** `los_metrics`

### SQL Expressions

**Measures:**
- `Discharges` → `COUNT(hsp_account_id)`
- `Average LOS (days)` → `ROUND(AVG(los_observed_days), 2)`
- `Average Inpatient LOS (days)` → `ROUND(AVG(los_inpatient_days), 2)`
- `Average Observation LOS (hours)` → `ROUND(AVG(los_observation_hours), 2)`
- `Long LOS Rate` → `ROUND(AVG(long_los_flag) * 100, 1)`

**Filters:**
- `FY2026` → `fiscal_year = 2026`
- `FY2026 Q1-Q3` → `fiscal_year = 2026 AND fiscal_month BETWEEN 1 AND 9`
- `Inpatient Only` → `account_class_name = 'IP'`
- `Observation Only` → `account_class_name = 'OBSV'`

**Synonyms:**
- `hospital` → site, facility, op unit, governed op unit
- `payer_category` → financial class, payer, insurance
- `discharge_department_name` → department, unit, ward
- `los_observed_days` → length of stay, LOS, days in hospital

### Text Instructions

Recommended minimal set:
- Default to `los_observed_days` for LOS unless user specifies inpatient or observation
- Fiscal year starts September 1 (FM 1 = Sep, FM 12 = Aug)
- Unless specified, default to the most recent available fiscal year
- For average LOS, always use `ROUND(AVG(...), 2)`
- When comparing hospitals, order by: NMH, CDH, PH, MCH, LFH, KH, VH, DHG

## Benchmark Questions (14)

### Category 1: Volume & Throughput (Q1–Q3)

| Q | Question |
|---|---|
| Q1 | How many discharges occurred during FY2026 Q1-Q3? |
| Q2 | How did discharges break down by fiscal month during FY2026 Q1-Q3? |
| Q3 | Which hospitals had the highest discharge volumes? |

### Category 2: Average Length of Stay (Q4–Q6)

| Q | Question |
|---|---|
| Q4 | What is the average LOS across all inpatient encounters? |
| Q5 | What is the average LOS by hospital? |
| Q6 | What is the average LOS by discharge department? |

### Category 3: Trends (Q7–Q8)

| Q | Question |
|---|---|
| Q7 | Which fiscal month had the highest average LOS? |
| Q8 | Which fiscal month had the highest discharge volume? |

### Category 4: Segmentation (Q9–Q10)

| Q | Question |
|---|---|
| Q9 | How does LOS compare between IP and OBSV encounters? |
| Q10 | What is the average LOS by payer category? |

### Category 5: Ranking & Prioritization (Q11–Q12)

| Q | Question |
|---|---|
| Q11 | What are the top 10 departments by average LOS? |
| Q12 | How has hospital LOS changed year-over-year (FY2026 vs FY2025)? |

### Category 6: Critical Thinking (Q13–Q14)

| Q | Question |
|---|---|
| Q13 | What are the top 10 areas of LOS variation? |
| Q14 | What context would you provide a data analyst to ensure reproducible results? |
