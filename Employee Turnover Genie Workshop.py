# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Genie Agents Workshop — Employee Turnover
# MAGIC %md
# MAGIC # 🏥 Genie Agents Workshop — Employee Turnover
# MAGIC ## Building a Trusted Genie Space on Employee Turnover
# MAGIC
# MAGIC **Two-day workshop** · Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Workshop Objectives
# MAGIC 1. **Understand** how to scope a Genie space to one business domain (Employee Turnover)
# MAGIC 2. **Build** a production-quality Genie agent using workforce metrics data
# MAGIC 3. **Master** the 9 workstreams that make a Genie space trustworthy and reproducible
# MAGIC
# MAGIC ### Agenda Overview
# MAGIC
# MAGIC | # | Workstream | Focus |
# MAGIC |---|---|---|
# MAGIC | 0 | Domains & Discovery | Scope a Genie space to one business domain |
# MAGIC | 1 | Data Preparation | Gold views, metric views, UC metadata |
# MAGIC | 2 | SQL Expressions | Filters, measures, fields with synonyms |
# MAGIC | 3 | Example SQL Queries | Static + parameterized trusted asset queries |
# MAGIC | 4 | Text Instructions | Last resort — minimal and specific |
# MAGIC | 5 | AI/BI Dashboard | Wire curated metrics into governed dashboards |
# MAGIC | 6 | Benchmarks & Monitoring | Benchmark suite + weekly digest feedback loop |
# MAGIC | 7 | Data Access Model | Security, row/column controls, space permissions |
# MAGIC | 8 | Genie One | Surface approved spaces to end users |
# MAGIC
# MAGIC ### Our Domain: Employee Turnover
# MAGIC
# MAGIC **Business Context:**
# MAGIC This workshop uses workforce turnover data across operating units, job families, departments, and shifts. The primary metrics are Rolling 12-Month (R12) and 3-Month Annualized (R3MA) turnover rates, for both overall and first-year employees.
# MAGIC
# MAGIC **Source System:** `fact_workforce_metrics` (73 columns) — update to your own source table
# MAGIC
# MAGIC **Key Measures:**
# MAGIC * Overall Turnover Rate (R12) — Rolling 12-month turnover
# MAGIC * First Year Turnover (R12) — Rolling 12-month new hire turnover
# MAGIC * Overall Turnover (R3 Annualized) — 3-month annualized turnover
# MAGIC * First Year Turnover (R3 Annualized) — 3-month annualized new hire turnover

# COMMAND ----------

# DBTITLE 1,⚙️ SETUP: Environment Configuration
# MAGIC %md
# MAGIC # ⚙️ SETUP: Environment Configuration
# MAGIC
# MAGIC ### ⚠️ Before Running This Notebook
# MAGIC 1. **Set your catalog and schema** in the cell below — update `CATALOG` and `SCHEMA` to match your environment.
# MAGIC 2. **If you already have your own `fact_workforce_metrics` table**, skip the sample-data cells (cells marked ⚙️) and point the gold view and metric view at your own table instead.
# MAGIC 3. **If you do NOT have the table yet**, run the sample-data cells to create a synthetic `fact_workforce_metrics` table in your target schema so the entire notebook runs end-to-end.
# MAGIC
# MAGIC | Sample Table | Rows | Notes |
# MAGIC |---|---|---|
# MAGIC | `fact_workforce_metrics` | ~2,400 | Synthetic data — replace with your own table when ready |
# MAGIC
# MAGIC ### Data Structure
# MAGIC The fact table stores **multiple metric types in the same rows**, distinguished by `metric_nm`:
# MAGIC * `Rolling 12 Month Turnover Rate`
# MAGIC * `Rolling 12 Month First Year Employee Org Turnover`
# MAGIC * `3 Month Annualized First Year Turnover Rate`
# MAGIC * `3 Month Annualized Overall Turnover Rate`
# MAGIC
# MAGIC Each row has `numerator_num`, `denominator_num`, and `max_r12_month_num`. The turnover rate formula is:
# MAGIC ```
# MAGIC rate = SUM(numerator) / (SUM(denominator) / MAX(max_r12_months))
# MAGIC ```
# MAGIC
# MAGIC > **Running in a different environment?** All table references in this notebook flow from the `CATALOG` and `SCHEMA` variables defined once in the setup cell. Python cells use them directly; SQL cells reference them via `${catalog}.${schema_name}` widgets (created automatically). Update those two values at the top and the entire notebook follows.

# COMMAND ----------

# DBTITLE 1,⚙️ Create sample fact_workforce_metrics table
# ===========================================================================
# ⚙️ SAMPLE DATA SETUP
# ===========================================================================
# Creates a synthetic fact_workforce_metrics table so this notebook runs
# end-to-end without requiring access to a production data source.
#
# ❗ If you already have your own fact_workforce_metrics table, SKIP this cell
#   and point the gold view / metric view at your own table instead.
# ===========================================================================
# ~2,400 rows | 10+ OP units | 12 JFGs | 4 metric types | July 2026 focus

import random
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date
from itertools import product

random.seed(42)

CATALOG = "schauhan_workspace_catalog"   # <-- Update based on your access
SCHEMA  = "nwm_turnover_workshop"        # <-- Update based on your access

# Create widgets so SQL cells can reference via ${catalog} and ${schema_name}
dbutils.widgets.text("catalog", CATALOG, "Catalog")
dbutils.widgets.text("schema_name", SCHEMA, "Schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# ===== Reference Data =====

# Operating units (op_unit_name) — from benchmark questions
OP_UNITS = ["NMH", "CDH", "LFH", "MCH", "VWH", "KH", "HH", "PHSC", "RNSC", "MSC", "NMHC"]

# Department sub-orgs — R3MA set filters to these two
SUB_ORGS = {
    "NMH":  "NHN Hospital",
    "CDH":  "NHN Hospital",
    "LFH":  "NHN Hospital",
    "MCH":  "AMC Hospital",
    "VWH":  "AMC Hospital",
    "KH":   "AMC Hospital",
    "HH":   "AMC Hospital",
    "PHSC": "NM Physician Services",
    "RNSC": "NM Research",
    "MSC":  "NM Medical Staff",
    "NMHC": "NM Home Care",
}

# Department orgs
DEPT_ORGS = {
    "NHN Hospital": "Northwestern Hospital Network",
    "AMC Hospital": "Academic Medical Centers",
    "NM Physician Services": "NM Corporate",
    "NM Research": "NM Corporate",
    "NM Medical Staff": "NM Corporate",
    "NM Home Care": "NM Corporate",
}

# Job Family Groupers (JFG) — from benchmark questions
JFGS = [
    "Housekeeping and food services",
    "Executive Management",
    "Management-Healthcare",
    "Patient Care Support",
    "Nursing",
    "Allied Health",
    "IT & APP",
    "APP",
    "Administrative Support",
    "Pharmacy",
    "Laboratory",
    "Facilities and Engineering",
]

# Shifts
SHIFTS = ["1-Day", "2-Evening", "3-Night", "R-Rotating"]

# Departments at NMH with JFG = Patient Care Support (for Q9)
NMH_PCS_DEPTS = [
    "1400-Medicine 11W", "1465-CVT Stepdown 12G", "1420-Surgery 14E",
    "1430-Oncology 21W", "1450-Neuro ICU", "1460-Cardiac ICU",
    "1410-Ortho 17E", "1440-Transplant 15W", "1470-Rehab 8E",
    "1480-Emergency Dept", "1490-Labor & Delivery",
]

# Regions, business_units, types, geographies, sites
REGIONS = {"NMH": "Chicago", "CDH": "Suburban North", "LFH": "Suburban North",
           "MCH": "Suburban West", "VWH": "Suburban West", "KH": "Suburban South",
           "HH": "Suburban South", "PHSC": "Multi-site", "RNSC": "Chicago",
           "MSC": "Chicago", "NMHC": "Multi-site"}
BUSINESS_UNITS = {"NMH": "NMH Main Campus", "CDH": "CDH Campus", "LFH": "LFH Campus",
                  "MCH": "MCH Campus", "VWH": "VWH Campus", "KH": "KH Campus",
                  "HH": "HH Campus", "PHSC": "Physician Services", "RNSC": "Research",
                  "MSC": "Medical Staff Corp", "NMHC": "Home Care"}
DEPT_TYPES = ["Inpatient", "Ambulatory", "Emergency", "Perioperative", "Administrative"]
GEOGRAPHIES = ["Chicago", "Evanston", "Lake Forest", "Geneva", "Winfield",
               "Downers Grove", "Palos Heights", "Skokie"]
SITES = ["Main Hospital", "Medical Office Bldg", "Outpatient Center", "Research Tower"]

# Job families and job codes per JFG
JOB_FAMILIES = {
    "Housekeeping and food services": ["Housekeeping", "Food Service", "Laundry"],
    "Executive Management": ["C-Suite", "VP Operations"],
    "Management-Healthcare": ["Nurse Manager", "Clinical Manager", "Dept Director"],
    "Patient Care Support": ["CNA", "Patient Care Tech", "Unit Secretary"],
    "Nursing": ["Staff RN", "Charge RN", "Clinical Nurse Specialist"],
    "Allied Health": ["Respiratory Therapist", "Physical Therapist", "OT"],
    "IT & APP": ["Systems Analyst", "Network Engineer", "App Developer"],
    "APP": ["Physician Assistant", "Nurse Practitioner"],
    "Administrative Support": ["Admin Assistant", "Scheduler", "Registrar"],
    "Pharmacy": ["Pharmacist", "Pharmacy Tech"],
    "Laboratory": ["Medical Technologist", "Lab Assistant"],
    "Facilities and Engineering": ["HVAC Tech", "Electrician", "Plumber"],
}

# ===== Target Turnover Rates for July 2026 (from benchmark answers) =====
# R12 Overall by OP unit
R12_OVERALL = {"PHSC": 0.29, "MSC": 0.22, "NMHC": 0.20, "NMH": 0.17, "CDH": 0.16,
               "LFH": 0.15, "MCH": 0.14, "KH": 0.13, "VWH": 0.12, "HH": 0.10, "RNSC": 0.07}
# R12 First Year by OP unit (MSC highest at 114.3%)
R12_FY = {"MSC": 1.143, "PHSC": 0.55, "NMHC": 0.45, "NMH": 0.35, "CDH": 0.30,
          "LFH": 0.28, "MCH": 0.25, "KH": 0.20, "VWH": 0.15, "HH": 0.10, "RNSC": 0.0}
# R3MA Overall by OP unit (only NHN/AMC hospitals)
R3_OVERALL = {"NMH": 0.18, "CDH": 0.16, "LFH": 0.14, "MCH": 0.13, "KH": 0.11,
              "HH": 0.10, "VWH": 0.086}
# R3MA First Year by OP unit
R3_FY = {"LFH": 0.399, "NMH": 0.35, "CDH": 0.30, "MCH": 0.28, "KH": 0.25,
         "VWH": 0.22, "HH": 0.19}

# R12 Overall by JFG
R12_JFG_OVERALL = {
    "Housekeeping and food services": 0.294, "Facilities and Engineering": 0.22,
    "Patient Care Support": 0.21, "Administrative Support": 0.18, "Laboratory": 0.16,
    "Nursing": 0.14, "Pharmacy": 0.12, "Allied Health": 0.11, "APP": 0.09,
    "IT & APP": 0.08, "Management-Healthcare": 0.06, "Executive Management": 0.048
}
# R12 First Year by JFG
R12_JFG_FY = {
    "Housekeeping and food services": 0.653, "Facilities and Engineering": 0.45,
    "Patient Care Support": 0.40, "Administrative Support": 0.35, "Laboratory": 0.30,
    "Nursing": 0.25, "Pharmacy": 0.20, "Allied Health": 0.15, "APP": 0.10,
    "IT & APP": 0.08, "Management-Healthcare": 0.042, "Executive Management": 0.05
}

# R12 Overall by Shift
R12_SHIFT = {"2-Evening": 0.203, "3-Night": 0.19, "R-Rotating": 0.16, "1-Day": 0.13}

# R3MA Overall by JFG (NHN/AMC only)
R3_JFG_OVERALL = {
    "Housekeeping and food services": 0.288, "Facilities and Engineering": 0.20,
    "Patient Care Support": 0.19, "Administrative Support": 0.16, "Laboratory": 0.14,
    "Nursing": 0.12, "Pharmacy": 0.10, "Allied Health": 0.08, "APP": 0.0,
    "IT & APP": 0.0, "Management-Healthcare": 0.0, "Executive Management": 0.0
}
# R3MA First Year by JFG (NHN/AMC only)
R3_JFG_FY = {
    "Housekeeping and food services": 0.603, "Facilities and Engineering": 0.35,
    "Patient Care Support": 0.30, "Administrative Support": 0.20, "Laboratory": 0.15,
    "Nursing": 0.12, "Pharmacy": 0.08, "Allied Health": 0.05, "APP": 0.0,
    "IT & APP": 0.0, "Management-Healthcare": 0.0, "Executive Management": 0.0
}

# R3MA Overall by Shift (NHN/AMC only) — 3-Night & 2-Evening tied at 21.1%
R3_SHIFT = {"3-Night": 0.211, "2-Evening": 0.211, "R-Rotating": 0.17, "1-Day": 0.12}

# ===== Generate Rows =====
# Strategy: For each (op_unit, jfg, shift, metric_nm) at month = July 2026,
# set numerator/denominator to produce the target rates.
# rate = SUM(numerator) / (SUM(denominator) / MAX(max_r12_months))
# So for a single row: numerator = rate * denominator / max_r12_months

METRICS = [
    ("Rolling 12 Month Turnover Rate", 12),
    ("Rolling 12 Month First Year Employee Org Turnover", 12),
    ("3 Month Annualized First Year Turnover Rate", 3),
    ("3 Month Annualized Overall Turnover Rate", 3),
]

rows = []
emplid_counter = 100000

def make_row(op_unit, jfg, shift, metric_nm, max_r12, numerator, denominator,
             month_dt, dept_name=None, emplid=None):
    global emplid_counter
    if emplid is None:
        emplid_counter += 1
        emplid = emplid_counter
    sub_org = SUB_ORGS[op_unit]
    dept_org = DEPT_ORGS[sub_org]
    region = REGIONS[op_unit]
    bu = BUSINESS_UNITS[op_unit]
    jf_list = JOB_FAMILIES.get(jfg, ["General"])
    jf_name = random.choice(jf_list)
    dept_type = random.choice(DEPT_TYPES)
    geo = random.choice(GEOGRAPHIES)
    site = random.choice(SITES)
    cc_name = dept_name or f"{op_unit}-{jfg[:20]}-{shift[0]}"
    
    return (
        random.randint(1, 999),          # metric_id
        metric_nm,                         # metric_nm
        float(numerator),                  # numerator_num
        float(denominator),                # denominator_num
        month_dt,                          # week_dts (use month for simplicity)
        month_dt,                          # month_dts
        max_r12,                           # max_r12_month_num
        random.randint(1000, 9999),        # jobcode
        f"{jf_name} I",                    # job_code_name
        jfg[:30],                          # job_code_grouper
        random.randint(100, 999),          # job_family (numeric)
        jf_name,                           # job_family_name
        jfg,                               # job_Family_grouper
        bu,                                # business_unit
        random.randint(10000, 99999),      # cost_center_id
        cc_name,                           # cost_center_name
        random.randint(1, 20),             # region_id
        region,                            # region
        1,                                 # nm_health_system_id
        "Health System",                   # nm_health_system
        random.randint(100, 999),          # department_org_id
        dept_org,                          # department_org
        random.randint(1000, 9999),        # department_sub_org_id
        sub_org,                           # department_sub_org
        random.randint(100, 999),          # op_unit_id
        op_unit,                           # op_unit_name
        dept_type,                         # department_type
        geo,                               # department_geography
        site,                              # department_site
        emplid,                            # emplid
        f"Employee_{emplid}",              # empl_name
        shift,                             # shift
        f"Officer_{random.randint(1,50)}", # officercd_nm
        round(random.uniform(0, 30), 1),   # yrs_of_service
        round(random.uniform(0, 35), 1),   # yrs_of_experience
        # Manager hierarchy (mgr1 through mgr12, each with officercd_nm, emplid, name)
        *[val for i in range(1, 13) for val in (
            f"Mgr{i}_Officer", random.randint(50000, 59999), f"Manager_{i}_{random.randint(1,100)}"
        )],
        "2026-07-15 08:00:00",             # meta_inserted_datetime
        "2026-07-15 08:00:00",             # meta_updated_datetime
    )

month_july = date(2026, 7, 1)

# --- Generate R12 Overall + First Year by (op_unit, jfg, shift) ---
for op_unit in OP_UNITS:
    for jfg in JFGS:
        for shift in SHIFTS:
            base_denom = random.randint(8, 25)  # headcount per cell
            
            # R12 Overall: blend op_unit rate and jfg rate
            r_ou = R12_OVERALL.get(op_unit, 0.15)
            r_jfg = R12_JFG_OVERALL.get(jfg, 0.15)
            r_shift = R12_SHIFT.get(shift, 0.15)
            rate_r12 = (r_ou * 0.4 + r_jfg * 0.35 + r_shift * 0.25) + random.uniform(-0.01, 0.01)
            rate_r12 = max(0, rate_r12)
            num_r12 = round(rate_r12 * base_denom / 12, 4)
            rows.append(make_row(op_unit, jfg, shift, METRICS[0][0], 12, num_r12, base_denom, month_july))
            
            # R12 First Year
            r_fy_ou = R12_FY.get(op_unit, 0.25)
            r_fy_jfg = R12_JFG_FY.get(jfg, 0.25)
            rate_fy12 = (r_fy_ou * 0.45 + r_fy_jfg * 0.55) + random.uniform(-0.02, 0.02)
            rate_fy12 = max(0, rate_fy12)
            fy_denom = max(3, base_denom // 3)  # first-year is subset
            num_fy12 = round(rate_fy12 * fy_denom / 12, 4)
            rows.append(make_row(op_unit, jfg, shift, METRICS[1][0], 12, num_fy12, fy_denom, month_july))
            
            # R3MA Overall (only NHN/AMC hospitals)
            if SUB_ORGS[op_unit] in ("NHN Hospital", "AMC Hospital"):
                r3_ou = R3_OVERALL.get(op_unit, 0.13)
                r3_jfg = R3_JFG_OVERALL.get(jfg, 0.10)
                r3_shift = R3_SHIFT.get(shift, 0.14)
                rate_r3 = (r3_ou * 0.4 + r3_jfg * 0.35 + r3_shift * 0.25) + random.uniform(-0.01, 0.01)
                rate_r3 = max(0, rate_r3)
                num_r3 = round(rate_r3 * base_denom / 3, 4)
                rows.append(make_row(op_unit, jfg, shift, METRICS[3][0], 3, num_r3, base_denom, month_july))
                
                # R3MA First Year
                r3_fy_ou = R3_FY.get(op_unit, 0.25)
                r3_fy_jfg = R3_JFG_FY.get(jfg, 0.15)
                rate_r3fy = (r3_fy_ou * 0.45 + r3_fy_jfg * 0.55) + random.uniform(-0.02, 0.02)
                rate_r3fy = max(0, rate_r3fy)
                num_r3fy = round(rate_r3fy * fy_denom / 3, 4)
                rows.append(make_row(op_unit, jfg, shift, METRICS[2][0], 3, num_r3fy, fy_denom, month_july))

# --- Generate NMH Patient Care Support departments (for Q9) ---
# R12: 1400-Medicine 11W = 80%, R3MA: 1465-CVT Stepdown 12G = 113.5%
Q9_R12_RATES = {
    "1400-Medicine 11W": 0.80, "1465-CVT Stepdown 12G": 0.75,
    "1420-Surgery 14E": 0.60, "1430-Oncology 21W": 0.50,
    "1450-Neuro ICU": 0.45, "1460-Cardiac ICU": 0.40,
    "1410-Ortho 17E": 0.35, "1440-Transplant 15W": 0.30,
    "1470-Rehab 8E": 0.25, "1480-Emergency Dept": 0.20,
    "1490-Labor & Delivery": 0.15,
}
Q9_R3_RATES = {
    "1465-CVT Stepdown 12G": 1.135, "1400-Medicine 11W": 0.90,
    "1420-Surgery 14E": 0.70, "1430-Oncology 21W": 0.55,
    "1450-Neuro ICU": 0.45, "1460-Cardiac ICU": 0.40,
    "1410-Ortho 17E": 0.35, "1440-Transplant 15W": 0.30,
    "1470-Rehab 8E": 0.25, "1480-Emergency Dept": 0.20,
    "1490-Labor & Delivery": 0.15,
}

for dept in NMH_PCS_DEPTS:
    headcount = random.randint(12, 30)  # all >= 10 employees
    for shift in SHIFTS:
        # R12 Overall
        r = Q9_R12_RATES[dept]
        num = round(r * headcount / 12, 4)
        for emp_i in range(headcount // 4 + 1):  # multiple employees per dept
            rows.append(make_row("NMH", "Patient Care Support", shift,
                                METRICS[0][0], 12, num / (headcount // 4 + 1),
                                headcount // (headcount // 4 + 1),
                                month_july, dept_name=dept))
        # R3MA Overall
        r3 = Q9_R3_RATES[dept]
        num3 = round(r3 * headcount / 3, 4)
        for emp_i in range(headcount // 4 + 1):
            rows.append(make_row("NMH", "Patient Care Support", shift,
                                METRICS[3][0], 3, num3 / (headcount // 4 + 1),
                                headcount // (headcount // 4 + 1),
                                month_july, dept_name=dept))

# --- Add some historical months (Jan-Jun 2026) for time-series context ---
for m in range(1, 7):
    month_dt = date(2026, m, 1)
    for op_unit in ["NMH", "CDH", "LFH", "MCH"]:
        for jfg in random.sample(JFGS, 4):
            shift = random.choice(SHIFTS)
            denom = random.randint(10, 20)
            rate = R12_OVERALL.get(op_unit, 0.15) + random.uniform(-0.03, 0.03)
            num = round(max(0, rate) * denom / 12, 4)
            rows.append(make_row(op_unit, jfg, shift, METRICS[0][0], 12, num, denom, month_dt))

print(f"Generated {len(rows)} rows")

# ===== Build Schema =====
# 73 columns matching the customer DDL
schema_fields = [
    StructField("metric_id", IntegerType()),
    StructField("metric_nm", StringType()),
    StructField("numerator_num", DoubleType()),
    StructField("denominator_num", DoubleType()),
    StructField("week_dts", DateType()),
    StructField("month_dts", DateType()),
    StructField("max_r12_month_num", IntegerType()),
    StructField("jobcode", IntegerType()),
    StructField("job_code_name", StringType()),
    StructField("job_code_grouper", StringType()),
    StructField("job_family", IntegerType()),
    StructField("job_family_name", StringType()),
    StructField("job_Family_grouper", StringType()),
    StructField("business_unit", StringType()),
    StructField("cost_center_id", IntegerType()),
    StructField("cost_center_name", StringType()),
    StructField("region_id", IntegerType()),
    StructField("region", StringType()),
    StructField("nm_health_system_id", IntegerType()),
    StructField("nm_health_system", StringType()),
    StructField("department_org_id", IntegerType()),
    StructField("department_org", StringType()),
    StructField("department_sub_org_id", IntegerType()),
    StructField("department_sub_org", StringType()),
    StructField("op_unit_id", IntegerType()),
    StructField("op_unit_name", StringType()),
    StructField("department_type", StringType()),
    StructField("department_geography", StringType()),
    StructField("department_site", StringType()),
    StructField("emplid", IntegerType()),
    StructField("empl_name", StringType()),
    StructField("shift", StringType()),
    StructField("officercd_nm", StringType()),
    StructField("yrs_of_service", DoubleType()),
    StructField("yrs_of_experience", DoubleType()),
]
# Manager hierarchy: mgr1 through mgr12
for i in range(1, 13):
    schema_fields.append(StructField(f"nm_mgr{i}_officercd_nm", StringType()))
    schema_fields.append(StructField(f"nm_mgr{i}_emplid", IntegerType()))
    schema_fields.append(StructField(f"nm_mgr{i}_name", StringType()))

schema_fields.append(StructField("meta_inserted_datetime", StringType()))
schema_fields.append(StructField("meta_updated_datetime", StringType()))

schema = StructType(schema_fields)

df = spark.createDataFrame(rows, schema=schema)
df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_workforce_metrics")

count = spark.table(f"{CATALOG}.{SCHEMA}.fact_workforce_metrics").count()
print(f"\n✅ fact_workforce_metrics: {count:,} rows")
print(f"   Schema: {len(schema_fields)} columns")
print(f"   Catalog: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,⚙️ Rebuild calibrated sample data (v2)
# ===========================================================================
# ⚙️ SAMPLE DATA CALIBRATION V2
# ===========================================================================
# ❗ Skip this cell if you are using your own fact_workforce_metrics table.
#
# Rebuilds fact_workforce_metrics so benchmark rankings/rates are driven by a
# calibrated cross-product instead of a blended-rate heuristic.
#
# Key fixes:
# 1. Preserve benchmark rankings across OP unit, JFG, and shift
# 2. Set true zero-rate combinations where required
# 3. Keep Q9 department rows tiny in denominator so they satisfy headcount >= 10
#    without inflating NMH / Patient Care Support rollups

import random
from pyspark.sql.types import *
from datetime import date

random.seed(42)

rows = []
emplid_counter = 300000
month_july = date(2026, 7, 1)

R3_OP_UNITS = [op for op in OP_UNITS if SUB_ORGS[op] in ("NHN Hospital", "AMC Hospital")]


def balancing_weights(targets, base):
    weights = {k: 1.0 for k in targets}
    vals = list(targets.values())
    n = len(vals)
    total = sum(vals)
    mean = total / n
    if abs(mean - base) < 1e-9:
        return weights
    if mean < base:
        key = max(targets, key=targets.get)
        t = targets[key]
        extra = (base * n - total) / (t - base)
    else:
        key = min(targets, key=targets.get)
        t = targets[key]
        extra = (total - base * n) / (base - t)
    weights[key] += float(extra)
    return weights


def add_metric_grid(metric_nm, max_r12, op_targets, base, jfg_targets=None, shift_targets=None, op_units=None):
    if op_units is None:
        op_units = list(op_targets.keys())

    if jfg_targets is None:
        jfg_targets = {"Administrative Support": base}
    if shift_targets is None:
        shift_targets = {"1-Day": base}

    op_weights = balancing_weights({k: op_targets[k] for k in op_units}, base)
    jfg_weights = balancing_weights(jfg_targets, base)
    shift_weights = balancing_weights(shift_targets, base)

    for op_unit in op_units:
        for jfg in jfg_targets:
            for shift in shift_targets:
                denominator = 25.0 * op_weights[op_unit] * jfg_weights[jfg] * shift_weights[shift]
                rate = op_targets[op_unit]
                rate *= jfg_targets[jfg] / base
                rate *= shift_targets[shift] / base
                numerator = rate * denominator / max_r12
                rows.append(
                    make_row(
                        op_unit=op_unit,
                        jfg=jfg,
                        shift=shift,
                        metric_nm=metric_nm,
                        max_r12=max_r12,
                        numerator=numerator,
                        denominator=denominator,
                        month_dt=month_july,
                    )
                )


# =========================================================
# R12 Overall — exact marginals for OP Unit + JFG + Shift
# =========================================================
R12_BASE = 0.17
add_metric_grid(
    metric_nm="Rolling 12 Month Turnover Rate",
    max_r12=12,
    op_targets=R12_OVERALL,
    base=R12_BASE,
    jfg_targets=R12_JFG_OVERALL,
    shift_targets=R12_SHIFT,
    op_units=OP_UNITS,
)

# =========================================================
# R12 First Year — exact marginals for OP Unit + JFG
# =========================================================
R12_FY_BASE = 0.25
add_metric_grid(
    metric_nm="Rolling 12 Month First Year Employee Org Turnover",
    max_r12=12,
    op_targets=R12_FY,
    base=R12_FY_BASE,
    jfg_targets=R12_JFG_FY,
    shift_targets={"1-Day": R12_FY_BASE},
    op_units=OP_UNITS,
)

# =========================================================
# R3 Overall — exact marginals for OP Unit + JFG + Shift
# =========================================================
R3_BASE = 0.14
add_metric_grid(
    metric_nm="3 Month Annualized Overall Turnover Rate",
    max_r12=3,
    op_targets=R3_OVERALL,
    base=R3_BASE,
    jfg_targets=R3_JFG_OVERALL,
    shift_targets=R3_SHIFT,
    op_units=R3_OP_UNITS,
)

# =========================================================
# R3 First Year — exact marginals for OP Unit + JFG
# =========================================================
R3_FY_BASE = 0.25
add_metric_grid(
    metric_nm="3 Month Annualized First Year Turnover Rate",
    max_r12=3,
    op_targets=R3_FY,
    base=R3_FY_BASE,
    jfg_targets=R3_JFG_FY,
    shift_targets={"1-Day": R3_FY_BASE},
    op_units=R3_OP_UNITS,
)

# =========================================================
# Q9 Department calibration rows
# Keep denominators tiny so these rows do not distort OP unit / JFG totals.
# Headcount still works because it uses COUNT(DISTINCT emplid).
# =========================================================
Q9_R12_RATES = {
    "1400-Medicine 11W": 0.80,
    "1465-CVT Stepdown 12G": 0.75,
    "1420-Surgery 14E": 0.60,
    "1430-Oncology 21W": 0.50,
    "1450-Neuro ICU": 0.45,
    "1460-Cardiac ICU": 0.40,
    "1410-Ortho 17E": 0.35,
    "1440-Transplant 15W": 0.30,
    "1470-Rehab 8E": 0.25,
    "1480-Emergency Dept": 0.20,
    "1490-Labor & Delivery": 0.15,
}
Q9_R3_RATES = {
    "1465-CVT Stepdown 12G": 1.135,
    "1400-Medicine 11W": 0.90,
    "1420-Surgery 14E": 0.70,
    "1430-Oncology 21W": 0.55,
    "1450-Neuro ICU": 0.45,
    "1460-Cardiac ICU": 0.40,
    "1410-Ortho 17E": 0.35,
    "1440-Transplant 15W": 0.30,
    "1470-Rehab 8E": 0.25,
    "1480-Emergency Dept": 0.20,
    "1490-Labor & Delivery": 0.15,
}

for dept in NMH_PCS_DEPTS:
    for i in range(12):
        denominator_r12 = 0.10
        numerator_r12 = Q9_R12_RATES[dept] * denominator_r12 / 12
        rows.append(
            make_row(
                op_unit="NMH",
                jfg="Patient Care Support",
                shift="1-Day",
                metric_nm="Rolling 12 Month Turnover Rate",
                max_r12=12,
                numerator=numerator_r12,
                denominator=denominator_r12,
                month_dt=month_july,
                dept_name=dept,
                emplid=400000 + (NMH_PCS_DEPTS.index(dept) * 100) + i,
            )
        )

        denominator_r3 = 0.10
        numerator_r3 = Q9_R3_RATES[dept] * denominator_r3 / 3
        rows.append(
            make_row(
                op_unit="NMH",
                jfg="Patient Care Support",
                shift="1-Day",
                metric_nm="3 Month Annualized Overall Turnover Rate",
                max_r12=3,
                numerator=numerator_r3,
                denominator=denominator_r3,
                month_dt=month_july,
                dept_name=dept,
                emplid=500000 + (NMH_PCS_DEPTS.index(dept) * 100) + i,
            )
        )

# Lightweight history for time-series context
for month_num in range(1, 7):
    month_dt = date(2026, month_num, 1)
    for op_unit in ["NMH", "CDH", "LFH", "MCH"]:
        rate = R12_OVERALL[op_unit]
        rows.append(
            make_row(
                op_unit=op_unit,
                jfg="Administrative Support",
                shift="1-Day",
                metric_nm="Rolling 12 Month Turnover Rate",
                max_r12=12,
                numerator=rate * 20 / 12,
                denominator=20,
                month_dt=month_dt,
            )
        )

# Rebuild the table
schema_fields = [
    StructField("metric_id", IntegerType()),
    StructField("metric_nm", StringType()),
    StructField("numerator_num", DoubleType()),
    StructField("denominator_num", DoubleType()),
    StructField("week_dts", DateType()),
    StructField("month_dts", DateType()),
    StructField("max_r12_month_num", IntegerType()),
    StructField("jobcode", IntegerType()),
    StructField("job_code_name", StringType()),
    StructField("job_code_grouper", StringType()),
    StructField("job_family", IntegerType()),
    StructField("job_family_name", StringType()),
    StructField("job_Family_grouper", StringType()),
    StructField("business_unit", StringType()),
    StructField("cost_center_id", IntegerType()),
    StructField("cost_center_name", StringType()),
    StructField("region_id", IntegerType()),
    StructField("region", StringType()),
    StructField("nm_health_system_id", IntegerType()),
    StructField("nm_health_system", StringType()),
    StructField("department_org_id", IntegerType()),
    StructField("department_org", StringType()),
    StructField("department_sub_org_id", IntegerType()),
    StructField("department_sub_org", StringType()),
    StructField("op_unit_id", IntegerType()),
    StructField("op_unit_name", StringType()),
    StructField("department_type", StringType()),
    StructField("department_geography", StringType()),
    StructField("department_site", StringType()),
    StructField("emplid", IntegerType()),
    StructField("empl_name", StringType()),
    StructField("shift", StringType()),
    StructField("officercd_nm", StringType()),
    StructField("yrs_of_service", DoubleType()),
    StructField("yrs_of_experience", DoubleType()),
]
for i in range(1, 13):
    schema_fields.append(StructField(f"nm_mgr{i}_officercd_nm", StringType()))
    schema_fields.append(StructField(f"nm_mgr{i}_emplid", IntegerType()))
    schema_fields.append(StructField(f"nm_mgr{i}_name", StringType()))

schema_fields.append(StructField("meta_inserted_datetime", StringType()))
schema_fields.append(StructField("meta_updated_datetime", StringType()))

schema = StructType(schema_fields)

calibrated_df = spark.createDataFrame(rows, schema=schema)
calibrated_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_workforce_metrics")

print(f"✅ Rebuilt calibrated sample data: {len(rows):,} rows")
print(f"   Table: {CATALOG}.{SCHEMA}.fact_workforce_metrics")
print("   Calibration fixes: exact marginals for benchmark dimensions, true zeros, tiny-denominator Q9 rows")

# COMMAND ----------

# DBTITLE 1,Workstream 1 — Data Preparation
# MAGIC %md
# MAGIC # Workstream 1 — Data Preparation
# MAGIC ## Gold View + Metric View
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Architecture (T-Decision 1)
# MAGIC ```
# MAGIC fact_workforce_metrics (73 columns)
# MAGIC     │
# MAGIC     ├──▶ Gold View (all columns + comments)  →  Ad-hoc analyst queries
# MAGIC     │
# MAGIC     └──▶ Metric View (~15 dimensions + 5 measures)  →  Genie space (sole data source)
# MAGIC ```
# MAGIC
# MAGIC **Why two layers?**
# MAGIC * The gold view preserves the full schema for power users
# MAGIC * The metric view curates only the dimensions that matter for turnover questions (T-Decision 2)
# MAGIC * The customer’s `source.*` wildcard exposes 73 dimensions (including 36 manager columns) — too noisy for Genie

# COMMAND ----------

# DBTITLE 1,Create gold_turnover_all view (all 73 columns)
# MAGIC %sql
# MAGIC -- Gold View: All columns from fact_workforce_metrics with comments
# MAGIC -- This is the full-schema reference view — NOT used in the Genie space
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ${catalog}.${schema_name}.gold_turnover_all AS
# MAGIC SELECT
# MAGIC   metric_id,
# MAGIC   metric_nm,
# MAGIC   numerator_num,
# MAGIC   denominator_num,
# MAGIC   week_dts,
# MAGIC   month_dts,
# MAGIC   max_r12_month_num,
# MAGIC   jobcode,
# MAGIC   job_code_name,
# MAGIC   job_code_grouper,
# MAGIC   job_family,
# MAGIC   job_family_name,
# MAGIC   `job_Family_grouper`,
# MAGIC   business_unit,
# MAGIC   cost_center_id,
# MAGIC   cost_center_name,
# MAGIC   region_id,
# MAGIC   region,
# MAGIC   nm_health_system_id,
# MAGIC   nm_health_system,
# MAGIC   department_org_id,
# MAGIC   department_org,
# MAGIC   department_sub_org_id,
# MAGIC   department_sub_org,
# MAGIC   op_unit_id,
# MAGIC   op_unit_name,
# MAGIC   department_type,
# MAGIC   department_geography,
# MAGIC   department_site,
# MAGIC   emplid,
# MAGIC   empl_name,
# MAGIC   shift,
# MAGIC   officercd_nm,
# MAGIC   yrs_of_service,
# MAGIC   yrs_of_experience,
# MAGIC   nm_mgr1_officercd_nm, nm_mgr1_emplid, nm_mgr1_name,
# MAGIC   nm_mgr2_officercd_nm, nm_mgr2_emplid, nm_mgr2_name,
# MAGIC   nm_mgr3_officercd_nm, nm_mgr3_emplid, nm_mgr3_name,
# MAGIC   nm_mgr4_officercd_nm, nm_mgr4_emplid, nm_mgr4_name,
# MAGIC   nm_mgr5_officercd_nm, nm_mgr5_emplid, nm_mgr5_name,
# MAGIC   nm_mgr6_officercd_nm, nm_mgr6_emplid, nm_mgr6_name,
# MAGIC   nm_mgr7_officercd_nm, nm_mgr7_emplid, nm_mgr7_name,
# MAGIC   nm_mgr8_officercd_nm, nm_mgr8_emplid, nm_mgr8_name,
# MAGIC   nm_mgr9_officercd_nm, nm_mgr9_emplid, nm_mgr9_name,
# MAGIC   nm_mgr10_officercd_nm, nm_mgr10_emplid, nm_mgr10_name,
# MAGIC   nm_mgr11_officercd_nm, nm_mgr11_emplid, nm_mgr11_name,
# MAGIC   nm_mgr12_officercd_nm, nm_mgr12_emplid, nm_mgr12_name,
# MAGIC   meta_inserted_datetime,
# MAGIC   meta_updated_datetime
# MAGIC FROM ${catalog}.${schema_name}.fact_workforce_metrics;

# COMMAND ----------

# DBTITLE 1,Create turnover_metrics metric view (∼15 dimensions + 5 measures)
# MAGIC %sql
# MAGIC -- Metric View: Curated dimensions + 5 measures (4 customer + headcount)
# MAGIC -- This is the SOLE data source for the Genie space (T-Decision 2-3)
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ${catalog}.${schema_name}.turnover_metrics
# MAGIC   WITH METRICS
# MAGIC   LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC source: ${catalog}.${schema_name}.fact_workforce_metrics
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: op_unit
# MAGIC     expr: op_unit_name
# MAGIC     comment: "Site or facility within the health system (e.g., NMH, CDH, LFH)."
# MAGIC     display_name: Operating Unit
# MAGIC     synonyms:
# MAGIC       - hospital
# MAGIC       - facility
# MAGIC       - site
# MAGIC       - campus
# MAGIC       - op unit
# MAGIC
# MAGIC   - name: business_unit
# MAGIC     expr: business_unit
# MAGIC     comment: "Organizational entity within the health system."
# MAGIC     display_name: Business Unit
# MAGIC
# MAGIC   - name: region
# MAGIC     expr: region
# MAGIC     comment: "Geographic area designation (e.g., Chicago, Suburban North)."
# MAGIC     display_name: Region
# MAGIC
# MAGIC   - name: department_org
# MAGIC     expr: department_org
# MAGIC     comment: "Highest-level parent entity within the health system structure."
# MAGIC     display_name: Department Organization
# MAGIC     synonyms:
# MAGIC       - org
# MAGIC
# MAGIC   - name: department_sub_org
# MAGIC     expr: department_sub_org
# MAGIC     comment: "Operational categorization under each org (e.g., NHN Hospital, AMC Hospital)."
# MAGIC     display_name: Department Sub-Organization
# MAGIC     synonyms:
# MAGIC       - sub org
# MAGIC       - sub organization
# MAGIC
# MAGIC   - name: department_type
# MAGIC     expr: department_type
# MAGIC     comment: "Categorization based on care delivery setting (e.g., Inpatient, Ambulatory)."
# MAGIC     display_name: Department Type
# MAGIC
# MAGIC   - name: cost_center
# MAGIC     expr: cost_center_name
# MAGIC     comment: "Department name associated with each cost center."
# MAGIC     display_name: Cost Center / Department
# MAGIC     synonyms:
# MAGIC       - department
# MAGIC       - dept
# MAGIC       - cost center
# MAGIC
# MAGIC   - name: job_family_grouper
# MAGIC     expr: job_Family_grouper
# MAGIC     comment: "Grouping of related job families with similar skills and qualifications (e.g., Nursing, Patient Care Support)."
# MAGIC     display_name: Job Family Grouper
# MAGIC     synonyms:
# MAGIC       - JFG
# MAGIC       - job family group
# MAGIC       - job group
# MAGIC
# MAGIC   - name: job_family
# MAGIC     expr: job_family_name
# MAGIC     comment: "Specific job family within a grouper (e.g., Staff RN, CNA)."
# MAGIC     display_name: Job Family
# MAGIC
# MAGIC   - name: job_code_grouper
# MAGIC     expr: job_code_grouper
# MAGIC     comment: "Grouping of related job codes."
# MAGIC     display_name: Job Code Grouper
# MAGIC
# MAGIC   - name: shift
# MAGIC     expr: shift
# MAGIC     comment: "Work shift period (1-Day, 2-Evening, 3-Night, R-Rotating)."
# MAGIC     display_name: Shift
# MAGIC     synonyms:
# MAGIC       - work shift
# MAGIC       - job shift
# MAGIC
# MAGIC   - name: month
# MAGIC     expr: month_dts
# MAGIC     comment: "Month in which the metric was recorded. Use for time filtering (e.g., month = '2026-07-01')."
# MAGIC     display_name: Month
# MAGIC     synonyms:
# MAGIC       - month date
# MAGIC       - reporting month
# MAGIC       - metric month
# MAGIC
# MAGIC   - name: department_geography
# MAGIC     expr: department_geography
# MAGIC     comment: "City or neighborhood where the department is physically located."
# MAGIC     display_name: Department Geography
# MAGIC
# MAGIC   - name: department_site
# MAGIC     expr: department_site
# MAGIC     comment: "Building name or street address of the department."
# MAGIC     display_name: Department Site
# MAGIC
# MAGIC measures:
# MAGIC   - name: overall_turnover_r12
# MAGIC     expr: |-
# MAGIC       COALESCE(
# MAGIC         SUM(numerator_num) FILTER (WHERE metric_nm = 'Rolling 12 Month Turnover Rate') /
# MAGIC         NULLIF(
# MAGIC           COALESCE(
# MAGIC             SUM(denominator_num) FILTER (WHERE metric_nm = 'Rolling 12 Month Turnover Rate') /
# MAGIC             NULLIF(MAX(max_r12_month_num) FILTER (WHERE metric_nm = 'Rolling 12 Month Turnover Rate'), 0),
# MAGIC             1
# MAGIC           ),
# MAGIC           0
# MAGIC         ),
# MAGIC         1
# MAGIC       )
# MAGIC     comment: "Rolling 12-month overall turnover rate. Calculated as SUM(numerator) / (SUM(denominator) / MAX(max_r12_months)) for the Rolling 12 Month Turnover Rate metric."
# MAGIC     display_name: Overall Turnover Rate (R12)
# MAGIC     synonyms:
# MAGIC       - turnover rate
# MAGIC       - overall turnover
# MAGIC       - rolling 12
# MAGIC       - R12
# MAGIC       - annual turnover
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC
# MAGIC   - name: first_year_turnover_r12
# MAGIC     expr: |-
# MAGIC       COALESCE(
# MAGIC         SUM(numerator_num) FILTER (WHERE metric_nm = 'Rolling 12 Month First Year Employee Org Turnover') /
# MAGIC         NULLIF(
# MAGIC           COALESCE(
# MAGIC             SUM(denominator_num) FILTER (WHERE metric_nm = 'Rolling 12 Month First Year Employee Org Turnover') /
# MAGIC             NULLIF(MAX(max_r12_month_num) FILTER (WHERE metric_nm = 'Rolling 12 Month First Year Employee Org Turnover'), 0),
# MAGIC             1
# MAGIC           ),
# MAGIC           0
# MAGIC         ),
# MAGIC         1
# MAGIC       )
# MAGIC     comment: "Rolling 12-month first-year employee turnover rate. Measures turnover among employees in their first year of employment."
# MAGIC     display_name: First Year Turnover (R12)
# MAGIC     synonyms:
# MAGIC       - first year turnover
# MAGIC       - new hire turnover
# MAGIC       - R12 first year
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC
# MAGIC   - name: first_year_turnover_r3
# MAGIC     expr: |-
# MAGIC       COALESCE(
# MAGIC         SUM(numerator_num) FILTER (WHERE metric_nm = '3 Month Annualized First Year Turnover Rate') /
# MAGIC         NULLIF(
# MAGIC           COALESCE(
# MAGIC             SUM(denominator_num) FILTER (WHERE metric_nm = '3 Month Annualized First Year Turnover Rate') /
# MAGIC             NULLIF(MAX(max_r12_month_num) FILTER (WHERE metric_nm = '3 Month Annualized First Year Turnover Rate'), 0),
# MAGIC             1
# MAGIC           ),
# MAGIC           0
# MAGIC         ),
# MAGIC         1
# MAGIC       )
# MAGIC     comment: "3-month annualized first-year employee turnover rate."
# MAGIC     display_name: First Year Turnover (R3 Annualized)
# MAGIC     synonyms:
# MAGIC       - 3 month first year
# MAGIC       - R3 first year
# MAGIC       - annualized first year
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC
# MAGIC   - name: overall_turnover_r3
# MAGIC     expr: |-
# MAGIC       COALESCE(
# MAGIC         SUM(numerator_num) FILTER (WHERE metric_nm = '3 Month Annualized Overall Turnover Rate') /
# MAGIC         NULLIF(
# MAGIC           COALESCE(
# MAGIC             SUM(denominator_num) FILTER (WHERE metric_nm = '3 Month Annualized Overall Turnover Rate') /
# MAGIC             NULLIF(MAX(max_r12_month_num) FILTER (WHERE metric_nm = '3 Month Annualized Overall Turnover Rate'), 0),
# MAGIC             1
# MAGIC           ),
# MAGIC           0
# MAGIC         ),
# MAGIC         1
# MAGIC       )
# MAGIC     comment: "3-month annualized overall turnover rate."
# MAGIC     display_name: Overall Turnover (R3 Annualized)
# MAGIC     synonyms:
# MAGIC       - 3 month turnover
# MAGIC       - R3MA
# MAGIC       - R3
# MAGIC       - annualized turnover
# MAGIC       - 3 month annualized
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 2
# MAGIC
# MAGIC   - name: headcount
# MAGIC     expr: COUNT(DISTINCT emplid)
# MAGIC     comment: "Distinct employee count. Use in HAVING to filter for departments or groups with minimum staffing levels (e.g., HAVING MEASURE(headcount) >= 10)."
# MAGIC     display_name: Employee Headcount
# MAGIC     synonyms:
# MAGIC       - employees
# MAGIC       - employee count
# MAGIC       - staff count
# MAGIC       - head count
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,UC Metadata: COMMENT ON for gold view and metric view
# Add rich COMMENT ON descriptions for UC Discover search
# Uses CATALOG and SCHEMA defined in the setup cell above

# Gold view comments
spark.sql(f"""
COMMENT ON TABLE {CATALOG}.{SCHEMA}.gold_turnover_all IS
'Gold-layer view of workforce metrics with all 73 columns from fact_workforce_metrics. Contains turnover numerators, denominators, employee details, job hierarchy, 12-level manager hierarchy, and organizational structure. Use for ad-hoc analyst queries requiring the full schema. For Genie-powered turnover analysis, use the turnover_metrics metric view instead.'
""")

# Metric view comments
spark.sql(f"""
COMMENT ON TABLE {CATALOG}.{SCHEMA}.turnover_metrics IS
'Metric view for Employee Turnover analysis. Contains 14 curated dimensions (operating unit, job family grouper, department, shift, month, etc.) and 5 measures: Overall Turnover Rate R12, First Year Turnover R12, Overall Turnover R3 Annualized, First Year Turnover R3 Annualized, and Employee Headcount. Covers rolling 12-month and 3-month annualized rates across operating units. Source: fact_workforce_metrics.'
""")

# Fact table comments
spark.sql(f"""
COMMENT ON TABLE {CATALOG}.{SCHEMA}.fact_workforce_metrics IS
'Fact table containing workforce metrics by employee, month, and metric type. Each row has numerator_num and denominator_num for one of 4 turnover rate calculations. The metric_nm column distinguishes: Rolling 12 Month Turnover Rate, Rolling 12 Month First Year Employee Org Turnover, 3 Month Annualized First Year Turnover Rate, 3 Month Annualized Overall Turnover Rate.'
""")

print("✅ COMMENT ON applied to:")
print(f"   {CATALOG}.{SCHEMA}.gold_turnover_all")
print(f"   {CATALOG}.{SCHEMA}.turnover_metrics")
print(f"   {CATALOG}.{SCHEMA}.fact_workforce_metrics")

# COMMAND ----------

# DBTITLE 1,Domain tags for UC Discover
# MAGIC %sql
# MAGIC -- Tag all Turnover assets for UC Discover categorization
# MAGIC ALTER TABLE ${catalog}.${schema_name}.fact_workforce_metrics SET TAGS ('domain' = 'operations');
# MAGIC ALTER VIEW ${catalog}.${schema_name}.gold_turnover_all SET TAGS ('domain' = 'operations');
# MAGIC ALTER VIEW ${catalog}.${schema_name}.turnover_metrics SET TAGS ('domain' = 'operations');

# COMMAND ----------

# DBTITLE 1,Workstream 2 — Metric View Validation
# MAGIC %md
# MAGIC # Workstream 2 — Metric View Validation
# MAGIC ## Verify the metric view produces your actual benchmark results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC We run each of the 20 benchmark questions against `turnover_metrics` using `MEASURE()` syntax.
# MAGIC Record the results to establish your baseline metrics.
# MAGIC
# MAGIC ### ❗ Benchmark Questions ≠ Example Queries
# MAGIC
# MAGIC | | Benchmark Questions (below) | Example Queries (Workstream 3) |
# MAGIC |---|---|---|
# MAGIC | **Purpose** | Validate measures produce correct results | Teach Genie SQL patterns it can generalize from |
# MAGIC | **Count** | As many as needed (20 for Turnover) | **2–3 carefully chosen** |
# MAGIC | **Where they live** | This notebook (run → verify → score) | Registered in the Genie space as trusted assets |
# MAGIC | **SQL style** | `MEASURE()` on `turnover_metrics` (validation) | `MEASURE()` on `turnover_metrics` (same syntax) |
# MAGIC
# MAGIC **Why 2–3 beats 20:** Genie generalizes from examples — one query with `GROUP BY op_unit` teaches it to group by any dimension. Too many examples over-constrain the agent (it pattern-matches instead of reasoning) and create a maintenance burden.

# COMMAND ----------

# DBTITLE 1,Validation: R12 Overall Turnover by OP Unit (Q1-Q2)
# MAGIC %sql
# MAGIC -- R12 Q1-Q2: Which OP unit has the highest/lowest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   op_unit,
# MAGIC   MEASURE(overall_turnover_r12) AS overall_turnover_r12
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY overall_turnover_r12 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R12 First Year Turnover by OP Unit (Q3-Q4)
# MAGIC %sql
# MAGIC -- R12 Q3-Q4: Which OP unit has the highest/lowest first year turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   op_unit,
# MAGIC   MEASURE(first_year_turnover_r12) AS first_year_turnover_r12
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY first_year_turnover_r12 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R12 Overall Turnover by JFG (Q5-Q6)
# MAGIC %sql
# MAGIC -- R12 Q5-Q6: Which JFG has highest/lowest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   job_family_grouper,
# MAGIC   MEASURE(overall_turnover_r12) AS overall_turnover_r12
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY overall_turnover_r12 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R12 First Year Turnover by JFG (Q7-Q8)
# MAGIC %sql
# MAGIC -- R12 Q7-Q8: Which JFG has highest/lowest first year turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   job_family_grouper,
# MAGIC   MEASURE(first_year_turnover_r12) AS first_year_turnover_r12
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY first_year_turnover_r12 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R12 Q9 — NMH dept with JFG=Patient Care Support, 10+ employees
# MAGIC %sql
# MAGIC -- R12 Q9: Which department at NMH with JFG Patient Care Support and 10+ employees has highest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   cost_center AS department,
# MAGIC   MEASURE(overall_turnover_r12) AS overall_turnover_r12,
# MAGIC   MEASURE(headcount) AS employee_count
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND op_unit = 'NMH'
# MAGIC   AND job_family_grouper = 'Patient Care Support'
# MAGIC GROUP BY ALL
# MAGIC HAVING MEASURE(headcount) >= 10
# MAGIC ORDER BY overall_turnover_r12 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R12 Overall Turnover by Shift (Q10)
# MAGIC %sql
# MAGIC -- R12 Q10: Which shift has the highest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   shift,
# MAGIC   MEASURE(overall_turnover_r12) AS overall_turnover_r12
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY overall_turnover_r12 DESC;

# COMMAND ----------

# DBTITLE 1,R3MA Benchmark Validation (Set 2)
# MAGIC %md
# MAGIC # R3MA Benchmark Validation (Set 2)
# MAGIC **Filters:** Month = July 2026, SUB ORG = NHN Hospital, AMC Hospital

# COMMAND ----------

# DBTITLE 1,Validation: R3MA Overall Turnover by OP Unit (Q1-Q2)
# MAGIC %sql
# MAGIC -- R3MA Q1-Q2: Which OP unit has highest/lowest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   op_unit,
# MAGIC   MEASURE(overall_turnover_r3) AS overall_turnover_r3
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY overall_turnover_r3 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R3MA First Year Turnover by OP Unit (Q3-Q4)
# MAGIC %sql
# MAGIC -- R3MA Q3-Q4: Which OP unit has highest/lowest first year turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   op_unit,
# MAGIC   MEASURE(first_year_turnover_r3) AS first_year_turnover_r3
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY first_year_turnover_r3 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R3MA Overall Turnover by JFG (Q5-Q6)
# MAGIC %sql
# MAGIC -- R3MA Q5-Q6: Which JFG has highest/lowest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   job_family_grouper,
# MAGIC   MEASURE(overall_turnover_r3) AS overall_turnover_r3
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY overall_turnover_r3 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R3MA First Year Turnover by JFG (Q7-Q8)
# MAGIC %sql
# MAGIC -- R3MA Q7-Q8: Which JFG has highest/lowest first year turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   job_family_grouper,
# MAGIC   MEASURE(first_year_turnover_r3) AS first_year_turnover_r3
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY first_year_turnover_r3 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R3MA Q9 — NMH dept with JFG=Patient Care Support, 10+ employees
# MAGIC %sql
# MAGIC -- R3MA Q9: Which department at NMH with JFG Patient Care Support and 10+ employees has highest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   cost_center AS department,
# MAGIC   MEASURE(overall_turnover_r3) AS overall_turnover_r3,
# MAGIC   MEASURE(headcount) AS employee_count
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND op_unit = 'NMH'
# MAGIC   AND job_family_grouper = 'Patient Care Support'
# MAGIC   AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC HAVING MEASURE(headcount) >= 10
# MAGIC ORDER BY overall_turnover_r3 DESC;

# COMMAND ----------

# DBTITLE 1,Validation: R3MA Overall Turnover by Shift (Q10)
# MAGIC %sql
# MAGIC -- R3MA Q10: Which shift has highest overall turnover?
# MAGIC -- Run to discover your results
# MAGIC
# MAGIC SELECT
# MAGIC   shift,
# MAGIC   MEASURE(overall_turnover_r3) AS overall_turnover_r3
# MAGIC FROM ${catalog}.${schema_name}.turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC   AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY overall_turnover_r3 DESC;

# COMMAND ----------

# DBTITLE 1,Workstream 3-4 — Genie Space Configuration
# MAGIC %md
# MAGIC # Workstreams 3-4 — Genie Space: Example Queries + Text Instructions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Source
# MAGIC **1 metric view only:** `turnover_metrics` (per T-Decision 4)
# MAGIC
# MAGIC ## Example SQL Queries (3 — not 20!)
# MAGIC Choose queries that **teach different patterns**, not different questions. Genie generalizes from 2–3 well-chosen examples better than from an exhaustive list. Each example below teaches a distinct SQL pattern:
# MAGIC
# MAGIC ### E1: Basic — Overall Turnover by OP Unit (R12)
# MAGIC ```sql
# MAGIC SELECT op_unit, MEASURE(overall_turnover_r12) AS turnover_rate
# MAGIC FROM turnover_metrics
# MAGIC WHERE month = '2026-07-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY turnover_rate DESC
# MAGIC ```
# MAGIC
# MAGIC ### E2: Hard — NMH Dept with JFG + Headcount Filter (Q9)
# MAGIC ```sql
# MAGIC SELECT cost_center AS department, MEASURE(overall_turnover_r12) AS turnover_rate, MEASURE(headcount) AS employees
# MAGIC FROM turnover_metrics
# MAGIC WHERE month = '2026-07-01' AND op_unit = 'NMH' AND job_family_grouper = 'Patient Care Support'
# MAGIC GROUP BY ALL
# MAGIC HAVING MEASURE(headcount) >= 10
# MAGIC ORDER BY turnover_rate DESC
# MAGIC ```
# MAGIC
# MAGIC ### E3: R3MA with Sub-Org Filter
# MAGIC ```sql
# MAGIC SELECT op_unit, MEASURE(overall_turnover_r3) AS turnover_rate
# MAGIC FROM turnover_metrics
# MAGIC WHERE month = '2026-07-01' AND department_sub_org IN ('NHN Hospital', 'AMC Hospital')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY turnover_rate DESC
# MAGIC ```
# MAGIC
# MAGIC ## Text Instructions (5 — copy-paste to Genie space)
# MAGIC
# MAGIC ### I1: Default Measure
# MAGIC > When the user asks about "turnover" or "turnover rate" without specifying a time window, use overall_turnover_r12 (rolling 12 month).
# MAGIC
# MAGIC ### I2: R3MA Routing
# MAGIC > When the user mentions "3 month", "3-month", "annualized", "R3", or "R3MA", use the R3 measures (overall_turnover_r3 or first_year_turnover_r3).
# MAGIC
# MAGIC ### I3: First Year Routing
# MAGIC > When the user mentions "first year", "new hire", or "first-year" turnover, use the first year turnover measures. When combined with "3 month" use first_year_turnover_r3; otherwise use first_year_turnover_r12.
# MAGIC
# MAGIC ### I4: Internal Columns
# MAGIC > Do not filter by metric_nm, numerator_num, or denominator_num. The measures already apply the correct metric filters internally.
# MAGIC
# MAGIC ### I5: Headcount Threshold
# MAGIC > When the user asks about departments, cost centers, or job families "with N or more employees", use HAVING MEASURE(headcount) >= N to filter low-volume groups.
# MAGIC
# MAGIC ### I6: Default Time Period
# MAGIC > When the user does not specify a time period, use the most recent available month.
# MAGIC
# MAGIC ## Filters
# MAGIC | Name | Expression |
# MAGIC |---|---|
# MAGIC | NHN + AMC Hospitals | `turnover_metrics.department_sub_org IN ('NHN Hospital', 'AMC Hospital')` |
# MAGIC
# MAGIC ## Starter Questions (5)
# MAGIC 1. Which operating unit has the highest overall turnover rate?
# MAGIC 2. What is the first year turnover rate by job family grouper?
# MAGIC 3. Which department at NMH with 10+ employees has the highest turnover?
# MAGIC 4. How does 3-month annualized turnover compare across operating units for NHN and AMC hospitals?
# MAGIC 5. Which shift has the highest overall turnover rate?

# COMMAND ----------

# DBTITLE 1,Create Genie Space via SDK
# Create the Turnover Genie Space
from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()

# Auto-discover a serverless warehouse
warehouses = list(w.warehouses.list())
wh = next((wh for wh in warehouses if wh.warehouse_type and "SERVERLESS" in str(wh.warehouse_type)), warehouses[0] if warehouses else None)

if wh:
    print(f"Using warehouse: {wh.name} ({wh.id})")
    
    space = w.genie.create_space(
        title="Employee Turnover Agent",
        description="Genie space for Employee Turnover analysis. Covers rolling 12-month (R12) and 3-month annualized (R3MA) turnover rates across operating units, job families, departments, and shifts."  # <-- Update description for your org,
        warehouse_id=wh.id,
        serialized_space=json.dumps({"version": 1})
    )
    print(f"\n✅ Genie Space created!")
    print(f"   ID: {space.space_id}")
    print(f"   URL: /genie/rooms/{space.space_id}")
    print(f"\n⚠️  Next steps (manual in UI):")
    print(f"   1. Add data source: {CATALOG}.{SCHEMA}.turnover_metrics")
    print(f"   2. Add 3 example SQL queries (see Workstream 3-4 cell above)")
    print(f"   3. Add 5 text instructions (see Workstream 3-4 cell above)")
    print(f"   4. Add 2 filters (July 2026, NHN+AMC Hospitals)")
    print(f"   5. Add 5 starter questions")
else:
    print("❌ No warehouse found. Create one first.")