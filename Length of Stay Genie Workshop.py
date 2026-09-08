# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Genie Agents Workshop — Length of Stay
# MAGIC %md
# MAGIC # 🏥 Genie Agents Workshop — Length of Stay (LOS)
# MAGIC ## Building a Trusted Genie Space on Length of Stay (LOS)
# MAGIC
# MAGIC **Two-day workshop** · Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Workshop Objectives
# MAGIC 1. **Understand** how to scope a Genie space to one business domain (Length of Stay)
# MAGIC 2. **Build** a production-quality Genie agent using your organization's encounter data
# MAGIC 3. **Master** the 9 workstreams that make a Genie space trustworthy and reproducible
# MAGIC
# MAGIC ### Agenda Overview
# MAGIC
# MAGIC | # | Workstream | Focus |
# MAGIC |---|---|---|
# MAGIC | 0 | Domains & Discovery | Scope a Genie space to one business domain |
# MAGIC | 1 | Data Preparation | Denormalized gold views, metric views, UC metadata |
# MAGIC | 2 | SQL Expressions | Filters, measures, fields with synonyms |
# MAGIC | 3 | Example SQL Queries | Static + parameterized trusted asset queries |
# MAGIC | 4 | Text Instructions | Last resort — minimal and specific |
# MAGIC | 5 | AI/BI Dashboard | Wire curated metrics into governed dashboards |
# MAGIC | 6 | Benchmarks & Monitoring | Benchmark suite + weekly digest feedback loop |
# MAGIC | 7 | Data Access Model | Security, row/column controls, space permissions |
# MAGIC | 8 | Genie One | Surface approved spaces to end users |
# MAGIC
# MAGIC ### Schedule
# MAGIC | | Duration | Activity |
# MAGIC |---|---|---|
# MAGIC | **Day 1** | 2 hrs | Enablement — Concept walkthrough (workstreams 0-8) |
# MAGIC | | 2 hrs | Hands-on Build — workstreams 1-4 on your data |
# MAGIC | **Day 2** | 1 hr | Refresher — Recap, Q&A, set build targets |
# MAGIC | | Build | Hands-on — Continue workstreams 5-8 |
# MAGIC | | 30 min | Present your Genie space — Demo & peer review |

# COMMAND ----------

# DBTITLE 1,⚙️ SETUP: Environment Configuration
# MAGIC %md
# MAGIC # ⚙️ SETUP: Workshop Environment Configuration
# MAGIC
# MAGIC ### ⚠️ Before Running This Notebook
# MAGIC ```
# MAGIC your_catalog.your_schema.*  →  clinical_dm.hospital_encounters.*  /  edr.cds.*
# MAGIC ```
# MAGIC
# MAGIC The cells below create **sample tables** with 35,000 synthetic encounters so the entire workshop notebook runs end-to-end in this demo workspace. The sample data mirrors a production hospital schema with realistic LOS distributions across 8 hospitals, 15 departments, 7 payer categories, and fiscal years 2023-2026.
# MAGIC
# MAGIC | Sample Table | Rows | Production Equivalent |
# MAGIC |---|---|---|
# MAGIC | `nwm_los_workshop.rpt_vizient_and_hd_unioned` | 35,000 | `clinical_dm.hospital_encounters.rpt_vizient_and_hd_unioned` |
# MAGIC | `nwm_los_workshop.dim_date` | 1,461 | `edr.cds.dim_date` |
# MAGIC | `nwm_los_workshop.rpt_criticalcare_domainsummary` | 3,680 | `clinical_dm.critical_care.rpt_criticalcare_domainsummary` |

# COMMAND ----------

# DBTITLE 1,⚙️ Create sample tables
# ===========================================================================
# ⚙️ SAMPLE DATA SETUP — Comment out this entire cell if using your own data
# ===========================================================================
# Creates synthetic LOS data in your target catalog.schema
# 35K encounters | 8 hospitals | 15 departments | FY2023-FY2026

from pyspark.sql import functions as F

CATALOG = "schauhan_workspace_catalog"   # <-- Update based on your access
SCHEMA  = "nwm_los_workshop"             # <-- Update based on your access

# Create widgets so SQL cells can reference via ${catalog} and ${schema_name}
dbutils.widgets.text("catalog", CATALOG, "Catalog")
dbutils.widgets.text("schema_name", SCHEMA, "Schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --- dim_date (fiscal calendar: FY starts Sep 1) ---
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dim_date AS
WITH dates AS (
  SELECT explode(sequence(DATE'2022-09-01', DATE'2026-08-31', INTERVAL 1 DAY)) AS Date_Key
)
SELECT Date_Key,
  DATE_TRUNC('month', Date_Key) AS Month_Begin_Date,
  DATE_FORMAT(Date_Key, 'MMMM') AS MONTH_NAME,
  LEFT(DATE_FORMAT(Date_Key, 'MMMM'), 3) AS Month_Abbreviation,
  CASE WHEN MONTH(Date_Key) >= 9 THEN YEAR(Date_Key)+1 ELSE YEAR(Date_Key) END AS Fiscal_Year_number,
  CASE WHEN MONTH(Date_Key) >= 9 THEN MONTH(Date_Key)-8 ELSE MONTH(Date_Key)+4 END AS Fiscal_Month_Of_Year_Number
FROM dates
""")

# --- rpt_vizient_and_hd_unioned (main fact — 35K encounters) ---
df = spark.range(1, 35001).withColumnRenamed("id", "rid")
for i, seed in enumerate([1,3,7,11,13,17,19,23]):
    df = df.withColumn(f"h{i}", F.abs(F.hash(F.col("rid") * seed)))

hospitals = ["NMH","CDH","PH","MCH","LFH","KH","VH","DHG"]
depts = ["Medical/Surgical","Cardiology","Orthopedics","Neurology","Oncology",
         "Pulmonary","General Medicine","Obstetrics","Pediatrics","Transplant",
         "Rehabilitation","Trauma/Surgery","Gastroenterology","Nephrology","Infectious Disease"]
payer_c = ["Commercial","Medicare","Medicaid","Self-Pay","Medicare Advantage","Workers Comp","Other"]
payer_g = ["Commercial","Medicare","Medicaid","Self-Pay","Medicare","Workers Comp","Other"]
dx_names = ["Pneumonia","Heart Failure","COPD Exacerbation","Sepsis","Hip Replacement","Chest Pain","Stroke","Cellulitis"]

hosp_e = F.when(F.col("h0")%100<30,"NMH").when(F.col("h0")%100<48,"CDH").when(F.col("h0")%100<60,"PH") \
  .when(F.col("h0")%100<70,"MCH").when(F.col("h0")%100<80,"LFH").when(F.col("h0")%100<88,"KH") \
  .when(F.col("h0")%100<95,"VH").otherwise("DHG")
pay_e = F.when(F.col("h3")%100<35,F.lit(0)).when(F.col("h3")%100<65,F.lit(1)) \
  .when(F.col("h3")%100<80,F.lit(2)).when(F.col("h3")%100<88,F.lit(3)) \
  .when(F.col("h3")%100<95,F.lit(4)).when(F.col("h3")%100<98,F.lit(5)).otherwise(F.lit(6))

fact = (df
  .withColumn("hsp_account_id", F.col("rid")+1000000)
  .withColumn("PRIM_ENC_CSN_ID", F.col("rid")+2000000)
  .withColumn("encounter_type", F.lit("Hospital Encounter"))
  .withColumn("account_class_name", F.when(F.col("h1")%100<70,"IP").otherwise("OBSV"))
  .withColumn("account_subclass_name", F.when(F.col("account_class_name")=="IP","Inpatient").otherwise("Observation"))
  .withColumn("dept_Governed_Op_Unit", hosp_e)
  .withColumn("dept_Governed_Op_Unit_sort_order_pbi",
    F.when(F.col("dept_Governed_Op_Unit")=="NMH",1).when(F.col("dept_Governed_Op_Unit")=="CDH",2)
     .when(F.col("dept_Governed_Op_Unit")=="PH",3).when(F.col("dept_Governed_Op_Unit")=="MCH",4)
     .when(F.col("dept_Governed_Op_Unit")=="LFH",5).when(F.col("dept_Governed_Op_Unit")=="KH",6)
     .when(F.col("dept_Governed_Op_Unit")=="VH",7).otherwise(8))
  .withColumn("dept_Governed_Sub_Org", F.concat(F.col("dept_Governed_Op_Unit"), F.lit(" Sub-Org")))
  .withColumn("dept_Governed_Org", F.concat(F.col("dept_Governed_Op_Unit"), F.lit(" Org")))
  .withColumn("discharge_department_id", (F.col("h2")%15+101).cast("int"))
  .withColumn("discharge_department_name", F.element_at(F.array(*[F.lit(d) for d in depts]), (F.col("h2")%15+1).cast("int")))
  .withColumn("admitting_department_name", F.col("discharge_department_name"))
  .withColumn("first_ip_unit", F.col("discharge_department_name"))
  .withColumn("discharge_date", F.date_add(F.lit("2023-09-01"), (F.col("h4")%1000).cast("int")))
  .withColumn("_los", F.when(F.col("account_class_name")=="IP", 1.0+(F.col("h5")%150)/10.0).otherwise(0.5+(F.col("h5")%50)/10.0))
  .withColumn("admit_date", F.date_sub(F.col("discharge_date"), F.greatest(F.floor(F.col("_los")).cast("int"), F.lit(1))))
  .withColumn("hospital_observed_los_days", F.round(F.col("_los"),2))
  .withColumn("hospital_observed_los_rounded_days", F.ceil(F.col("_los")).cast("int"))
  .withColumn("vizient_observed_los_rounded_days", F.when(F.col("account_class_name")=="IP", F.ceil(F.col("_los")).cast("int")))
  .withColumn("inpatient_portion_of_los_in_days", F.when(F.col("account_class_name")=="IP", F.round(F.col("_los"),2)).otherwise(F.round(0.3+(F.col("h6")%20)/10.0,2)))
  .withColumn("observation_portion_of_los_in_hrs", F.when(F.col("account_class_name")=="OBSV", F.round(6.0+(F.col("h6")%600)/10.0,2)))
  .withColumn("hospital_observed_long_LOS_flag", F.when(F.col("_los")>10,1).otherwise(0))
  .withColumn("hospital_GMLOS_days", F.round(3.5+(F.col("h7")%30)/10.0,2))
  .withColumn("vizient_expected_los_days_current_risk_model", F.round(3.0+(F.col("h4")%50)/10.0,2))
  .withColumn("vizient_los_denominator_flag_current_risk_model", F.when((F.col("account_class_name")=="IP")&(F.col("h5")%100<85),1).otherwise(0))
  .withColumn("vizient_comparator_peer_group", F.lit("Teaching"))
  .withColumn("vizient_service_line_description_latest", F.element_at(F.array(*[F.lit(s) for s in ["Medicine","Surgery","Cardiology","Orthopedics","Neurosciences"]]), (F.col("h0")%5+1).cast("int")))
  .withColumn("vizient_sub_service_line_description_latest", F.col("discharge_department_name"))
  .withColumn("Vizient_Governed_Medical_Division", F.lit("Medicine"))
  .withColumn("Vizient_Governed_Medical_Department", F.col("discharge_department_name"))
  .withColumn("primary_financial_class", F.element_at(F.array(*[F.lit(p) for p in payer_c]), (pay_e+1).cast("int")))
  .withColumn("primary_coverage_payor", F.col("primary_financial_class"))
  .withColumn("account_financial_class_name", F.element_at(F.array(*[F.lit(p) for p in payer_g]), (pay_e+1).cast("int")))
  .withColumn("admit_provider_full_name", F.concat(F.lit("Dr. Provider_"), (F.col("h6")%200).cast("string")))
  .withColumn("discharge_attend_provider_full_name", F.concat(F.lit("Dr. Attending_"), (F.col("h7")%200).cast("string")))
  .withColumn("discharge_attend_provider_npi", (F.col("h7")%200+1000000).cast("long"))
  .withColumn("patient_age_at_admit", (20+F.col("h0")%75).cast("int"))
  .withColumn("sex", F.when(F.col("h1")%100<52,"Female").otherwise("Male"))
  .withColumn("race", F.element_at(F.array(*[F.lit(r) for r in ["White","Black","Hispanic","Asian","Other"]]), (F.col("h2")%5+1).cast("int")))
  .withColumn("ethnicity", F.lit("Non-Hispanic"))
  .withColumn("hospital_observed_mortality_flag", F.when(F.col("h0")%100<2,1).otherwise(0))
  .withColumn("complex_discharge_flag", F.when(F.col("h1")%100<8,1).otherwise(0))
  .withColumn("champ_encounter_flag", F.when(F.col("h2")%100<5,1).otherwise(0))
  .withColumn("patient_had_telehealth_consult_flag", F.when(F.col("h3")%100<3,1).otherwise(0))
  .withColumn("patient_has_sickle_cell_flag", F.when(F.col("h4")%100<1,1).otherwise(0))
  .withColumn("hospital_service", F.lit("Medicine"))
  .withColumn("primary_dx_code", F.concat(F.lit("J"),(10+F.col("h5")%90).cast("string"),F.lit("."),(F.col("h6")%9).cast("string")))
  .withColumn("primary_dx_name", F.element_at(F.array(*[F.lit(d) for d in dx_names]), (F.col("h0")%8+1).cast("int")))
  .withColumn("primary_procedure_code", F.lit(None).cast("string"))
  .withColumn("primary_procedure_name", F.lit(None).cast("string"))
  .withColumn("hospital_msdrg_code", (400+F.col("h7")%500).cast("string"))
  .withColumn("hospital_msdrg_description", F.lit("DRG Description"))
  .withColumn("hospital_case_mix_index_CMI", F.round(0.8+(F.col("h0")%35)/10.0,4))
  .withColumn("discharge_disposition_name", F.element_at(F.array(*[F.lit(d) for d in ["Home","Home Health","SNF","Rehab","Other"]]), (F.col("h1")%5+1).cast("int")))
  .withColumn("discharge_disposition_category_name", F.lit("Discharge"))
  .withColumn("total_careport_avoidable_days_per_csn", F.when(F.col("h2")%100<15, 1+F.col("h3")%4).otherwise(0))
  .withColumn("has_careport_avoidable_days_flag", F.when(F.col("h2")%100<15,1).otherwise(0))
  .withColumn("har_in_hd_dm_flag", F.lit(1))
  .withColumn("pat_id", F.col("rid")+3000000)
  .drop("rid","h0","h1","h2","h3","h4","h5","h6","h7","_los")
)
fact.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.rpt_vizient_and_hd_unioned")

# --- rpt_criticalcare_domainsummary (~15% of IP encounters) ---
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.rpt_criticalcare_domainsummary AS
SELECT PRIM_ENC_CSN_ID AS pat_enc_csn_id,
  CAST(1+ABS(hash(PRIM_ENC_CSN_ID*3))%3 AS INT) AS total_icu_visits_during_encounter,
  ROUND(1.0+ABS(hash(PRIM_ENC_CSN_ID*5))%100/10.0,1) AS total_days_spent_in_ICU_during_encounter,
  CASE WHEN ABS(hash(PRIM_ENC_CSN_ID*7))%100<40 THEN 1 ELSE 0 END AS spent_time_on_vent_flag,
  ROUND(ABS(hash(PRIM_ENC_CSN_ID*11))%50/10.0,1) AS total_vent_duration_in_days
FROM {CATALOG}.{SCHEMA}.rpt_vizient_and_hd_unioned
WHERE ABS(hash(hsp_account_id))%100<15 AND account_class_name='IP'
""")

print("✅ Sample data created successfully!")
for t in ["dim_date", "rpt_vizient_and_hd_unioned", "rpt_criticalcare_domainsummary"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{t}").collect()[0][0]
    print(f"   {t}: {cnt:,} rows")

# COMMAND ----------

# DBTITLE 1,⭐ SETUP: Create additional dimension tables
# ⭐ SETUP: Create 3 additional dimension tables from the DDL notebook
# These are the dimensions that were missing from our initial sample data.
# DDL cells: 3 (FYTD Through), 8 (Top Decile O/E), 11-12 (Provider Employment)

import random
from pyspark.sql import functions as F, types as T
from datetime import date, timedelta

# Uses CATALOG and SCHEMA defined in the setup cell above
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
HOSPITALS = ["NMH", "CDH", "PH", "MCH", "LFH", "KH", "VH", "DHG"]
random.seed(42)

# ═══════════════════════════════════════════════════════════
# 1. ref_fytd_through — Which fiscal months are complete for FYTD comparisons
#    DDL source: administrative_dm.reference.ref_fytd_through
#    PBI uses this to scope FYTD comparisons to only complete months
# ═══════════════════════════════════════════════════════════
fytd_rows = []
# FY2024: Sep 2023 - Aug 2024 (all complete)
for fm in range(1, 13):
    cal_month = (fm + 8) % 12 or 12
    cal_year = 2023 if fm <= 4 else 2024  # FM1=Sep2023, FM5=Jan2024
    if fm <= 4: cal_year = 2023
    else: cal_year = 2024
    # Correct: FM1=Sep → cal Sep 2023, FM4=Dec → cal Dec 2023, FM5=Jan → cal Jan 2024
    cal_year = 2023 if cal_month >= 9 else 2024
    fytd_rows.append((date(cal_year, cal_month, 1), 2024, fm, True))

# FY2025: Sep 2024 - Aug 2025 (all complete)
for fm in range(1, 13):
    cal_month = (fm + 8) % 12 or 12
    cal_year = 2024 if cal_month >= 9 else 2025
    fytd_rows.append((date(cal_year, cal_month, 1), 2025, fm, True))

# FY2026: Sep 2025 - Aug 2026 (FM1-8 complete, FM9=May in-progress, FM10-12 future)
for fm in range(1, 13):
    cal_month = (fm + 8) % 12 or 12
    cal_year = 2025 if cal_month >= 9 else 2026
    complete = fm <= 8  # Through Apr 2026 is complete; May (FM9) is partial
    fytd_rows.append((date(cal_year, cal_month, 1), 2026, fm, complete))

fytd_schema = T.StructType([
    T.StructField("Month_Begin_Date", T.DateType()),
    T.StructField("Fiscal_Year_Number", T.IntegerType()),
    T.StructField("Fiscal_Month_Of_Year_Number", T.IntegerType()),
    T.StructField("fytd_complete_flag", T.BooleanType()),
])
spark.createDataFrame(fytd_rows, schema=fytd_schema) \
    .write.mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.ref_fytd_through")
print(f"✅ ref_fytd_through: {len(fytd_rows)} rows (FY2024-FY2026)")

# ═══════════════════════════════════════════════════════════
# 2. ref_vizient_top_decile_oe — Vizient benchmark O/E targets by FY × hospital
#    DDL source: Cell 8 — derives from rpt_vizient_and_HD_unioned O/E columns
#    PBI cross-joins with risk model control (CURRENT, CURRENT-1, CURRENT-2)
#    Workshop simplification: only risk_model_id=1 (CURRENT) since sample data
#    only has current risk model columns
# ═══════════════════════════════════════════════════════════
oe_rows = []
for fy in [2024, 2025, 2026]:
    for hosp in HOSPITALS:
        # Top decile O/E benchmarks typically range 0.70-0.92
        # Larger hospitals tend to have tighter (lower) benchmarks
        base = 0.82 if hosp in ["NMH", "CDH"] else 0.85 if hosp in ["PH", "MCH", "LFH"] else 0.88
        oe_val = round(base + random.uniform(-0.03, 0.03), 3)
        oe_rows.append((fy, hosp, 1, "CURRENT", oe_val))

oe_schema = T.StructType([
    T.StructField("fiscal_year", T.IntegerType()),
    T.StructField("hospital", T.StringType()),
    T.StructField("risk_model_id", T.IntegerType()),
    T.StructField("risk_model_name", T.StringType()),
    T.StructField("vizient_top_decile_oe_value", T.DoubleType()),
])
spark.createDataFrame(oe_rows, schema=oe_schema) \
    .write.mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.ref_vizient_top_decile_oe")
print(f"✅ ref_vizient_top_decile_oe: {len(oe_rows)} rows ({len(HOSPITALS)} hospitals × 3 FYs)")

# ═══════════════════════════════════════════════════════════
# 3. ref_physician_employment — Provider NM-employed status (SCD Type 2 expanded)
#    DDL source: Cells 11-12 — SCD on ref_physician_employment, expanded by date
#    PBI joins via computed key: concat(npi, ' ', date_key) = provider_date
#    Workshop: pre-materialize the SCD expansion for each unique NPI in our fact data
# ═══════════════════════════════════════════════════════════
# Get unique discharge provider NPIs from our fact table
npis = [row.discharge_attend_provider_npi 
        for row in spark.table(f"{FULL_SCHEMA}.rpt_vizient_and_hd_unioned")
            .select("discharge_attend_provider_npi").distinct().collect()]

# For each NPI, assign NM-employed status (~70% employed, matches typical health system profile)
pe_rows = []
for npi in npis:
    nm_employed = 1 if random.random() < 0.70 else 0
    office_code = random.choice(["NMH", "CDH", "PH", "MCH", "LFH"]) if nm_employed else "COMMUNITY"
    pe_rows.append((int(npi), office_code, nm_employed))

pe_schema = T.StructType([
    T.StructField("npi", T.LongType()),
    T.StructField("Office_Code", T.StringType()),
    T.StructField("NM_Employed", T.IntegerType()),
])
spark.createDataFrame(pe_rows, schema=pe_schema) \
    .write.mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.ref_physician_employment")
print(f"✅ ref_physician_employment: {len(pe_rows)} providers ({sum(1 for r in pe_rows if r[2]==1)} NM-employed, {sum(1 for r in pe_rows if r[2]==0)} community)")

print(f"\n📊 All 3 dimension tables created in {FULL_SCHEMA}")
print("   Now the gold view can join 6 sources: fact + dim_date + critical_care + fytd + oe_benchmarks + provider_employment")

# COMMAND ----------

# DBTITLE 1,Workstream 0 — Domains & Discovery
# MAGIC %md
# MAGIC # Workstream 0 — Domains & Discovery
# MAGIC ## Concept: Scope a Genie space to one business domain
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC A Genie space works best when it's scoped to **one business domain** with a clear set of questions and a well-understood data model. Think of it like a subject-matter expert who knows everything about one topic — not a generalist.
# MAGIC
# MAGIC ### Why Domain Scoping Matters
# MAGIC * **Accuracy** — Fewer tables = less ambiguity for the AI to navigate
# MAGIC * **Trust** — Business users get consistent, validated answers
# MAGIC * **Governance** — Easier to control access and audit usage
# MAGIC * **Maintenance** — Smaller surface area = faster iteration
# MAGIC
# MAGIC ### Our Domain: Length of Stay (LOS)
# MAGIC
# MAGIC **Business Context:**
# MAGIC The organization tracks LOS across 7+ hospitals (NMH, CDH, PH, MCH, LFH, KH, VH, DHG) for both Inpatient (IP) and Observation (OBSV) encounters. The primary audience is operational leaders who need to:
# MAGIC * Monitor discharge volumes and throughput
# MAGIC * Track average LOS by hospital, department, and payer
# MAGIC * Identify trends across fiscal months
# MAGIC * Compare IP vs. OBSV performance
# MAGIC * Rank departments and hospitals for improvement priorities
# MAGIC
# MAGIC ### Source Tables in This Domain
# MAGIC
# MAGIC | Table | Purpose | Catalog.Schema |
# MAGIC |---|---|---|
# MAGIC | `rpt_vizient_and_hd_unioned` | **Main fact** — all hospital discharges with LOS, Vizient metrics, demographics | `clinical_dm.hospital_encounters` |
# MAGIC | `dim_date` | Date dimension with fiscal year/month mappings | `edr.cds` |
# MAGIC | `rpt_criticalcare_domainsummary` | ICU visits, ventilator, critical care flags | `clinical_dm.critical_care` |
# MAGIC | `vw_governed_hierarchy_vizient_sub_service_line` | Service line / specialty hierarchy | `administrative_dm.reference` |
# MAGIC | `ref_physician_employment` | Provider employment status (admit & discharge) | `administrative_dm.reference` |
# MAGIC | `ref_fytd_through` | Fiscal year-to-date through dates | `administrative_dm.reference` |
# MAGIC | `fact_hospitaldischarges` | Discharge lounge, additional discharge detail | `clinical_dm.hospital_encounters` |
# MAGIC | `fact_patient` | Patient demographics (birth date, state) | `clinical_dm.patient` |
# MAGIC
# MAGIC ### 💡 Discovery Exercise
# MAGIC Before building, always ask:
# MAGIC 1. What questions will users ask? (see our 14 benchmark questions below)
# MAGIC 2. Which tables answer those questions?
# MAGIC 3. Can we reduce from 8 tables to 1-2 gold views?
# MAGIC 4. Who are the users and what access do they need?

# COMMAND ----------

# DBTITLE 1,Explore the main fact table
# MAGIC %sql
# MAGIC -- 🔍 EXERCISE: Explore the main fact table structure
# MAGIC -- Understanding the source data is the first step in domain discovery
# MAGIC
# MAGIC DESCRIBE TABLE ${catalog}.${schema_name}.rpt_vizient_and_hd_unioned;

# COMMAND ----------

# DBTITLE 1,Explore the date dimension
# MAGIC %sql
# MAGIC -- 🔍 Explore the date dimension — critical for fiscal year/month filtering
# MAGIC SELECT * 
# MAGIC FROM ${catalog}.${schema_name}.dim_date 
# MAGIC WHERE Fiscal_Year_number = 2026 
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Workstream 1 — Data Preparation
# MAGIC %md
# MAGIC # Workstream 1 — Data Preparation
# MAGIC ## Concept: Denormalized gold views + metric views + UC metadata
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC Genie performs best with **denormalized, pre-joined gold views** rather than raw normalized tables. Think "one wide table per domain" rather than "join 6 tables on the fly."
# MAGIC
# MAGIC ### Why Denormalize for Genie?
# MAGIC * **Fewer joins** = less chance for the AI to generate incorrect SQL
# MAGIC * **Column names and descriptions** are all in one place for Genie to reason about
# MAGIC * **Metric views** let you define business-agreed calculations once, reuse everywhere
# MAGIC * **UC metadata** (descriptions, tags, PK/FK) gives Genie the context to understand your data
# MAGIC
# MAGIC ### The Gold View Pattern
# MAGIC ```
# MAGIC Raw sources → Bronze → Silver → Gold View (denormalized) → Genie Space
# MAGIC ```
# MAGIC
# MAGIC For LOS, the denormalized gold view joins:
# MAGIC * **Fact:** `rpt_vizient_and_hd_unioned` (encounters + LOS metrics)
# MAGIC * **Dim:** `dim_date` (fiscal year/month mappings)
# MAGIC * **Enrichment:** `rpt_criticalcare_domainsummary` (ICU flags)
# MAGIC * **Population filter:** `har_in_hd_dm_flag = 1` (All Hospital Discharges IP & OBSV)
# MAGIC
# MAGIC ### 💡 Best Practices
# MAGIC * Give every column a human-readable `COMMENT` — this is what Genie reads
# MAGIC * Use `ALTER TABLE ... SET TAGS` for primary/foreign key hints
# MAGIC * Avoid abbreviations in column names when possible (or add synonyms later in workstream 2)
# MAGIC * Pre-compute commonly requested metrics in the view rather than relying on Genie to calculate them

# COMMAND ----------

# DBTITLE 1,⭐ BUILD: Create the LOS Gold View
# MAGIC %sql
# MAGIC -- ⭐ BUILD: Create the denormalized gold view for the LOS Genie space
# MAGIC -- Rebuilt to match the customer's DDL notebook exactly (NM LOS PBI (DDL).dbc)
# MAGIC -- Only columns confirmed in the DDL AND present in our workshop sample tables are included.
# MAGIC -- 🏥 PRODUCTION comments mark columns from the DDL that require additional source tables.
# MAGIC --
# MAGIC -- Source DDL joins 6 tables in the Fact cell + 3 additional dimension tables.
# MAGIC -- Workshop sample covers 6 of 9 sources:
# MAGIC --   ✅ rpt_vizient_and_hd_unioned      (58 of ~100+ DDL columns in sample)
# MAGIC --   ✅ dim_date                         (6 of 6 DDL columns in sample)
# MAGIC --   ✅ rpt_criticalcare_domainsummary   (5 of ~35 DDL columns in sample)
# MAGIC --   ✅ ref_fytd_through                 (DDL Cell 3: FYTD completeness)
# MAGIC --   ✅ ref_vizient_top_decile_oe        (DDL Cell 8: Vizient O/E benchmarks)
# MAGIC --   ✅ ref_physician_employment         (DDL Cells 11-12: provider NM-employed status)
# MAGIC --   ❌ vw_ref_encounter_level_bmi       (not in sample — BMI data)
# MAGIC --   ❌ fact_hospitaldischarges           (not in sample — discharge lounge, admit NPI)
# MAGIC --   ❌ fact_hospitalencounters           (not in sample — Admitting_Base_Class_Name)
# MAGIC --   ❌ fact_patient                      (not in sample — birth_date, patient_state_province)
# MAGIC --
# MAGIC -- Population: All Hospital Discharges (IP & OBSV) where har_in_hd_dm_flag = 1
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ${catalog}.${schema_name}.gold_los AS
# MAGIC SELECT
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- ENCOUNTER IDENTIFIERS
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.hsp_account_id,
# MAGIC     vizhd.PRIM_ENC_CSN_ID,
# MAGIC     vizhd.encounter_type,
# MAGIC     vizhd.account_class_name,                                        -- 'IP' or 'OBSV'
# MAGIC     vizhd.account_subclass_name,
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- HOSPITAL & DEPARTMENT HIERARCHY  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.dept_Governed_Op_Unit                  AS hospital,         -- NMH, CDH, PH, MCH, LFH, KH, VH, DHG
# MAGIC     vizhd.dept_Governed_Op_Unit_sort_order_pbi   AS hospital_sort,
# MAGIC     vizhd.dept_Governed_Sub_Org                  AS sub_org,
# MAGIC     vizhd.dept_Governed_Org                      AS org,
# MAGIC     vizhd.discharge_department_id,
# MAGIC     vizhd.discharge_department_name,
# MAGIC     vizhd.admitting_department_name,
# MAGIC     vizhd.first_ip_unit,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → dept_Governed_Sub_Group, dept_Governed_Group,
# MAGIC     --    dept_Governed_Site, dept_Governed_Geography, dept_Governed_Type,
# MAGIC     --    dept_Governed_Sub_Org_sort_order_pbi, dept_Governed_Org_sort_order_pbi,
# MAGIC     --    discharge_room_name, discharge_location_id, discharge_location_name
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- DATE & FISCAL CALENDAR  (from dim_date)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.admit_date,
# MAGIC     vizhd.discharge_date,
# MAGIC     dd.Fiscal_Year_number                        AS fiscal_year,
# MAGIC     dd.Fiscal_Month_Of_Year_Number               AS fiscal_month,
# MAGIC     dd.MONTH_NAME                                AS month_name,
# MAGIC     dd.Month_Abbreviation                        AS month_abbreviation,
# MAGIC     dd.Month_Begin_Date                          AS month_begin_date,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → admit_time, CAST(LEFT(admit_time,2) AS INT) AS admit_hour,
# MAGIC     --    ip_admit_date, ip_admit_time, discharge_time, CAST(LEFT(discharge_time,2) AS INT) AS discharge_hour,
# MAGIC     --    observation_start_date, observation_start_time, observation_end_date, observation_end_time
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- LENGTH OF STAY METRICS  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.hospital_observed_los_days             AS los_observed_days,
# MAGIC     vizhd.hospital_observed_los_rounded_days     AS los_observed_rounded_days,
# MAGIC     vizhd.inpatient_portion_of_los_in_days       AS los_inpatient_days,
# MAGIC     vizhd.observation_portion_of_los_in_hrs      AS los_observation_hours,
# MAGIC     vizhd.hospital_observed_long_LOS_flag         AS long_los_flag,
# MAGIC     vizhd.hospital_GMLOS_days                    AS geometric_mean_los_days,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → hospital_observed_long_LOS_flag_based_on_rounded_days,
# MAGIC     --    observation_portion_of_los_in_hrs_grtr_48_flag, inpatient_portion_of_los_in_hrs,
# MAGIC     --    Transfer_in_HaH_Date, Transfer_in_HaH_Time, Transfer_HaH_back_to_facility_Date/Time,
# MAGIC     --    HaH_los_days, HaH_In_Facility_los_days, patient_used_hospital_at_home_during_HAR_flag
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- VIZIENT RISK-ADJUSTED METRICS  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.vizient_observed_los_rounded_days       AS vizient_observed_los_days,
# MAGIC     vizhd.vizient_expected_los_days_current_risk_model AS vizient_expected_los_days,
# MAGIC     vizhd.vizient_los_denominator_flag_current_risk_model AS vizient_los_denom_flag,
# MAGIC     vizhd.vizient_comparator_peer_group,
# MAGIC     vizhd.vizient_service_line_description_latest AS vizient_service_line,
# MAGIC     vizhd.vizient_sub_service_line_description_latest AS vizient_sub_service_line,
# MAGIC     vizhd.Vizient_Governed_Medical_Division       AS medical_division,
# MAGIC     vizhd.Vizient_Governed_Medical_Department     AS medical_department,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → Vizient_Governed_Center_Director,
# MAGIC     --    vizient_service_line_code_latest, vizient_sub_service_line_code_latest,
# MAGIC     --    vizient_los_denominator_flag_current_minus_one_risk_model,
# MAGIC     --    vizient_los_denominator_flag_current_minus_two_risk_model,
# MAGIC     --    vizient_expected_los_days_current_minus_one_risk_model,
# MAGIC     --    vizient_expected_los_days_current_minus_two_risk_model,
# MAGIC     --    HAR_last_available_risk_model_description,
# MAGIC     --    har_in_vizient_dm_flag
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- FINANCIAL / PAYER  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.primary_financial_class,
# MAGIC     vizhd.primary_coverage_payor,
# MAGIC     vizhd.account_financial_class_name            AS payer_category,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → secondary_coverage_payor, dual_eligibility_YN,
# MAGIC     --    primary_coverage_benefit_plan_name, primary_coverage_payor_subscriber_number,
# MAGIC     --    har_total_charges_greater_than_zero_flag
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- PROVIDER  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.admit_provider_full_name,
# MAGIC     vizhd.discharge_attend_provider_full_name     AS discharge_provider,
# MAGIC     vizhd.discharge_attend_provider_npi           AS discharge_provider_npi,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → admit_provider_specialty_1/2_description,
# MAGIC     --    discharge_attend_provider_specialty_1/2_description,
# MAGIC     --    procedure_provider_full_name, procedure_provider_specialty_1_description
# MAGIC     -- 🏥 PRODUCTION: Add from fact_hospitaldischarges → admit_provider_npi
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- PATIENT DEMOGRAPHICS  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.patient_age_at_admit,
# MAGIC     vizhd.sex,
# MAGIC     vizhd.race,
# MAGIC     vizhd.ethnicity,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → admission_source, language,
# MAGIC     --    under_resourced_community_flag, sexual_orientation, gender_identity,
# MAGIC     --    patient_postal_code, death_date, mrn, full_name
# MAGIC     -- 🏥 PRODUCTION: Add from fact_patient → birth_date AS patient_birth_date, patient_state_province
# MAGIC     -- 🏥 PRODUCTION: Add from fact_hospitalencounters → Admitting_Base_Class_Name
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- CLINICAL FLAGS  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.hospital_observed_mortality_flag         AS mortality_flag,
# MAGIC     vizhd.complex_discharge_flag,
# MAGIC     vizhd.champ_encounter_flag,
# MAGIC     vizhd.patient_had_telehealth_consult_flag      AS telehealth_flag,
# MAGIC     vizhd.patient_has_sickle_cell_flag             AS sickle_cell_flag,
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- DIAGNOSIS / PROCEDURE / DRG  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.hospital_service,
# MAGIC     vizhd.primary_dx_code,
# MAGIC     vizhd.primary_dx_name,
# MAGIC     vizhd.primary_procedure_code,
# MAGIC     vizhd.primary_procedure_name,
# MAGIC     vizhd.hospital_msdrg_code,
# MAGIC     vizhd.hospital_msdrg_description,
# MAGIC     vizhd.hospital_case_mix_index_CMI              AS case_mix_index,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → primary_procedure_code_type,
# MAGIC     --    vizient_msdrg_code, vizient_msdrg_description, vizient_msdrg_code_descritpion,
# MAGIC     --    hospital_msdrg_code_descritpion
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- DISCHARGE DISPOSITION  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.discharge_disposition_name,
# MAGIC     vizhd.discharge_disposition_category_name,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → discharged_to_facility_name
# MAGIC     -- 🏥 PRODUCTION: Add from fact_hospitaldischarges → discharge_lounge_used_flag,
# MAGIC     --    discharge_lounge_department_name, discharge_Lounge_Start_Date/Time,
# MAGIC     --    discharge_Lounge_End_Date/Time, discharge_Lounge_Duration_minutes
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- ICU / CRITICAL CARE  (from rpt_criticalcare_domainsummary)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     IF(cc.pat_enc_csn_id IS NOT NULL, 1, 0)        AS icu_encounter_flag,
# MAGIC     COALESCE(cc.total_icu_visits_during_encounter, 0) AS icu_visits,
# MAGIC     COALESCE(cc.total_days_spent_in_ICU_during_encounter, 0) AS icu_days,
# MAGIC     COALESCE(cc.spent_time_on_vent_flag, 0)        AS ventilator_flag,
# MAGIC     COALESCE(cc.total_vent_duration_in_days, 0)    AS ventilator_days,
# MAGIC     -- 🏥 PRODUCTION: Add from cc → total_days_spent_in_ICU_during_encounter_bins,
# MAGIC     --    ICUs_Time_spent_in_list, longest_icu_department_name, longest_icu_service_line_name,
# MAGIC     --    MICU_Flag, SICU_Flag, NSICU_FLAG, CTICU_FLAG, CCU_Flag,
# MAGIC     --    micu_returns, number_of_micu_returns_within_48_hrs_flag,
# MAGIC     --    first/last_micu_return_came_from_dept, first/last_micu_return_start_date/time,
# MAGIC     --    icu_returns, is_icu_direct_admit, icu_72hr_readmission_flag,
# MAGIC     --    total_vent_episode_count, first_vent_placed_date/time, last_vent_discontinued_date/time,
# MAGIC     --    ecmo_flag, tracheostomy_flag, pre_icu_los_days, post_icu_los_days,
# MAGIC     --    admit_to_trach_days, dialysis_flag, crrt_flag, hemodialysis_flag, dialysis_types_received
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- AVOIDABLE DAYS  (from vizhd)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     vizhd.total_careport_avoidable_days_per_csn    AS avoidable_days,
# MAGIC     vizhd.has_careport_avoidable_days_flag          AS has_avoidable_days_flag,
# MAGIC     -- 🏥 PRODUCTION: Add from vizhd → Number_of_midnights (OBSV only),
# MAGIC     --    greater_than_eql_to_2_midnights_flag, two_midnights_start/end_Date/time,
# MAGIC     --    has_two_midnights_and_avoidable_days_flag (OBSV only)
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- FYTD COMPLETENESS  (from ref_fytd_through — DDL Cell 3)
# MAGIC     -- Enables apples-to-apples FYTD comparisons by flagging complete months
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     fytd.fytd_complete_flag,
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- VIZIENT TOP DECILE BENCHMARK  (from ref_vizient_top_decile_oe — DDL Cell 8)
# MAGIC     -- Enables "Are we hitting top decile?" and O/E ratio vs benchmark questions
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     oe.vizient_top_decile_oe_value,
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- PROVIDER EMPLOYMENT  (from ref_physician_employment — DDL Cells 11-12)
# MAGIC     -- Enables "LOS for NM-employed vs community providers" segmentation
# MAGIC     -- DDL uses SCD Type 2 expansion: NPI × date → NM_Employed flag
# MAGIC     -- PBI joins via computed key: concat(npi, ' ', discharge_date)
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     dpe.NM_Employed                                AS discharge_provider_nm_employed,
# MAGIC     dpe.Office_Code                                AS discharge_provider_office_code
# MAGIC     -- 🏥 PRODUCTION: Add admit provider employment (requires admit_provider_npi from fact_hospitaldischarges):
# MAGIC     --   ape.NM_Employed AS admit_provider_nm_employed,
# MAGIC     --   ape.Office_Code AS admit_provider_office_code
# MAGIC
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- 🏥 PRODUCTION-ONLY: Tables not in workshop sample data
# MAGIC     -- ═══════════════════════════════════════════════════════════
# MAGIC     -- FROM vw_ref_encounter_level_bmi (JOIN ON PRIM_ENC_CSN_ID = pat_enc_csn_id):
# MAGIC     --   bmi, bmi_source, bmi_recorded_datetime
# MAGIC     -- FROM fact_hospitaldischarges (JOIN ON hsp_account_id = hsp_account_id):
# MAGIC     --   discharge_lounge_used_flag, discharge_lounge_department_name,
# MAGIC     --   discharge_Lounge_Start/End_Date/Time, discharge_Lounge_Duration_minutes,
# MAGIC     --   admit_provider_npi
# MAGIC     -- FROM fact_hospitalencounters (JOIN ON PRIM_ENC_CSN_ID = pat_enc_csn_id):
# MAGIC     --   Admitting_Base_Class_Name
# MAGIC     -- FROM fact_patient (JOIN ON pat_id = patient_id):
# MAGIC     --   birth_date AS patient_birth_date, patient_state_province
# MAGIC
# MAGIC FROM ${catalog}.${schema_name}.rpt_vizient_and_hd_unioned vizhd
# MAGIC -- Fiscal calendar (DDL Cell 2: Date Dim)
# MAGIC JOIN ${catalog}.${schema_name}.dim_date dd
# MAGIC   ON dd.Date_Key = vizhd.discharge_date
# MAGIC -- ICU enrichment (DDL Cell 1: Fact → LEFT JOIN cc)
# MAGIC LEFT JOIN ${catalog}.${schema_name}.rpt_criticalcare_domainsummary cc
# MAGIC   ON vizhd.PRIM_ENC_CSN_ID = cc.pat_enc_csn_id
# MAGIC -- FYTD completeness (DDL Cell 3: ref_fytd_through)
# MAGIC LEFT JOIN ${catalog}.${schema_name}.ref_fytd_through fytd
# MAGIC   ON fytd.Fiscal_Year_Number = dd.Fiscal_Year_number
# MAGIC   AND fytd.Fiscal_Month_Of_Year_Number = dd.Fiscal_Month_Of_Year_Number
# MAGIC -- Vizient top decile O/E benchmark (DDL Cell 8: Top Decile O/E)
# MAGIC LEFT JOIN ${catalog}.${schema_name}.ref_vizient_top_decile_oe oe
# MAGIC   ON oe.fiscal_year = dd.Fiscal_Year_number
# MAGIC   AND oe.hospital = vizhd.dept_Governed_Op_Unit
# MAGIC -- Discharge provider employment (DDL Cells 11-12: ref_physician_employment)
# MAGIC -- DDL uses SCD expansion + concat(npi,' ',date) key; workshop uses simplified NPI lookup
# MAGIC LEFT JOIN ${catalog}.${schema_name}.ref_physician_employment dpe
# MAGIC   ON dpe.npi = vizhd.discharge_attend_provider_npi
# MAGIC -- 🏥 PRODUCTION: Add these joins from DDL:
# MAGIC -- LEFT JOIN clinical_dm.patient.vw_ref_encounter_level_bmi bmi_ref
# MAGIC --   ON vizhd.PRIM_ENC_CSN_ID = bmi_ref.pat_enc_csn_id
# MAGIC -- LEFT JOIN clinical_dm.hospital_encounters.fact_hospitaldischarges hd
# MAGIC --   ON vizhd.hsp_account_id = hd.hsp_account_id
# MAGIC -- LEFT JOIN clinical_dm.hospital_encounters.fact_hospitalencounters he
# MAGIC --   ON vizhd.PRIM_ENC_CSN_ID = he.pat_enc_csn_id
# MAGIC -- LEFT JOIN clinical_dm.patient.fact_patient pat
# MAGIC --   ON vizhd.pat_id = pat.patient_id
# MAGIC -- 🏥 PRODUCTION: Add admit provider employment (requires admit_provider_npi from hd):
# MAGIC -- LEFT JOIN ref_physician_employment ape ON ape.npi = hd.admit_provider_npi
# MAGIC WHERE vizhd.har_in_hd_dm_flag = 1;  -- All Hospital Discharges (IP & OBSV)

# COMMAND ----------

# DBTITLE 1,Add UC metadata to the gold view
# ⭐ BUILD: Add Unity Catalog metadata so Genie understands the columns
# Column descriptions are the MOST important tuning lever for Genie accuracy
#
# NOTE: Views don't support ALTER COLUMN COMMENT.
# Strategy: Rich view-level COMMENT + metric view (Workstream 1.5) for per-column metadata.
# 🎯 For production: Recreate gold view with inline column comments:
#   CREATE VIEW ... (col1 COMMENT '...', col2 COMMENT '...') AS SELECT ...

# Uses CATALOG and SCHEMA defined in the setup cell above
VIEW = f"{CATALOG}.{SCHEMA}.gold_los"

# View-level comment with key column documentation
spark.sql(f"""
COMMENT ON TABLE {VIEW} IS
'Denormalized gold view for Length of Stay (LOS) analysis across hospital sites. Contains all hospital discharges (IP and OBSV) with LOS metrics, fiscal calendar, Vizient risk-adjusted measures, payer, provider, demographics, ICU enrichment, and avoidable days. Population: har_in_hd_dm_flag = 1. Key columns: hospital (site: NMH CDH PH MCH LFH KH VH DHG), fiscal_year (FY starts Sep 1), fiscal_month (1=Sep 12=Aug), account_class_name (IP or OBSV), los_observed_days (primary LOS in days), los_inpatient_days (IP days), los_observation_hours (OBSV hours), long_los_flag (1=exceeds threshold), payer_category (financial class), case_mix_index (CMI), icu_encounter_flag (1=had ICU visit).'
""")

print("✅ View-level COMMENT added with key column documentation")
print("📝 Per-column metadata with synonyms & formats → los_metrics metric view (Workstream 1.5)")

# COMMAND ----------

# DBTITLE 1,⭐ BUILD: Domain Tags for Discover Page
# MAGIC %sql
# MAGIC -- ⭐ BUILD: Tag all assets by domain so they appear categorized on the Discover page
# MAGIC -- Tags make assets searchable and filterable across the workspace
# MAGIC -- Note: Your workspace may have tag policies restricting allowed values — check with your admin
# MAGIC
# MAGIC -- Tag the schema
# MAGIC ALTER SCHEMA ${catalog}.${schema_name} SET TAGS ('domain' = 'operations');
# MAGIC
# MAGIC -- Tag each table and view
# MAGIC ALTER TABLE ${catalog}.${schema_name}.rpt_vizient_and_hd_unioned SET TAGS ('domain' = 'operations');
# MAGIC ALTER TABLE ${catalog}.${schema_name}.dim_date SET TAGS ('domain' = 'operations');
# MAGIC ALTER TABLE ${catalog}.${schema_name}.rpt_criticalcare_domainsummary SET TAGS ('domain' = 'operations');
# MAGIC ALTER VIEW  ${catalog}.${schema_name}.gold_los SET TAGS ('domain' = 'operations');
# MAGIC
# MAGIC -- Add rich descriptions for Discover page search
# MAGIC COMMENT ON SCHEMA ${catalog}.${schema_name} IS 'Length of Stay (LOS) domain. Encounter-level fact data, fiscal calendar, critical care enrichment, gold views, and metric views for hospital operations analytics.';
# MAGIC
# MAGIC COMMENT ON TABLE ${catalog}.${schema_name}.rpt_vizient_and_hd_unioned IS 'Hospital discharge fact table for LOS analysis. All IP and OBSV encounters with LOS metrics, Vizient risk-adjusted measures, payer, provider, demographics. Population: har_in_hd_dm_flag = 1. Source: Epic Clarity. Refresh: daily T-1.';
# MAGIC
# MAGIC COMMENT ON TABLE ${catalog}.${schema_name}.dim_date IS 'Date dimension with fiscal calendar. FY starts Sep 1. FM 1 = Sep, FM 12 = Aug. Covers FY2023-FY2026.';
# MAGIC
# MAGIC COMMENT ON TABLE ${catalog}.${schema_name}.rpt_criticalcare_domainsummary IS 'Critical care enrichment: ICU visits, ventilator usage for ~15% of IP encounters. Joins to fact on pat_enc_csn_id.';

# COMMAND ----------

# DBTITLE 1,Workstream 1.5 — UC Metric Views
# MAGIC %md
# MAGIC # Workstream 1.5 — UC Metric Views
# MAGIC ## Concept: Define reusable business metrics once, use everywhere
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC A **metric view** is a Unity Catalog object that separates **measure definitions** from **dimension groupings**. Unlike a regular view where aggregations are baked in, a metric view lets any consumer (Genie, dashboards, notebooks) query the same standardized measures with flexible GROUP BY.
# MAGIC
# MAGIC ### Why Metric Views Matter for Genie
# MAGIC * **One source of truth** — "Average LOS" is defined once, not re-derived per query
# MAGIC * **Synonyms built in** — Genie knows "LOS", "length of stay", "days in hospital" all mean `avg_los_days`
# MAGIC * **Formats built in** — Dashboards auto-format (2 decimal places, percentages, etc.)
# MAGIC * **Discover page integration** — Metric views appear as governed assets with full metadata
# MAGIC
# MAGIC ### Query Syntax
# MAGIC Metric views use `MEASURE()` instead of raw aggregations:
# MAGIC ```sql
# MAGIC SELECT hospital,
# MAGIC        MEASURE(discharges)    AS discharges,
# MAGIC        MEASURE(avg_los_days)  AS avg_los_days
# MAGIC FROM   catalog.schema.los_metrics
# MAGIC WHERE  fiscal_year = 2026
# MAGIC GROUP  BY ALL;
# MAGIC ```
# MAGIC
# MAGIC ### 💡 Metric View vs. SQL Expressions in Genie
# MAGIC | Feature | Metric View | Genie SQL Expression |
# MAGIC |---|---|---|
# MAGIC | Scope | Workspace-wide (UC) | Single Genie space |
# MAGIC | Reusable across dashboards | ✅ Yes | ❌ No |
# MAGIC | Synonyms | ✅ Built-in YAML | ✅ Configured in space |
# MAGIC | Formats (currency, %) | ✅ Built-in YAML | ❌ Manual |
# MAGIC | Version controlled | ✅ UC lineage | ❌ Space config only |
# MAGIC
# MAGIC **Best practice:** Define core metrics in a UC metric view, then register the metric view as a trusted asset in the Genie space.

# COMMAND ----------

# DBTITLE 1,⭐ BUILD: Create LOS Metric View
# MAGIC %sql
# MAGIC -- ⭐ BUILD: Create the LOS metric view with standardized measures and dimensions
# MAGIC -- This defines business-agreed metrics once and makes them reusable across
# MAGIC -- Genie spaces, AI/BI dashboards, and notebooks
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ${catalog}.${schema_name}.los_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC   version: 1.1
# MAGIC   source: ${catalog}.${schema_name}.gold_los
# MAGIC   comment: >
# MAGIC     Core Length of Stay metrics for Length of Stay operations.
# MAGIC     Covers all hospital discharges (IP & OBSV) across hospital sites.
# MAGIC     Fiscal year starts September 1.
# MAGIC
# MAGIC   dimensions:
# MAGIC     - name: hospital
# MAGIC       expr: hospital
# MAGIC       display_name: Hospital
# MAGIC       comment: Hospital site (NMH, CDH, PH, MCH, LFH, KH, VH, DHG)
# MAGIC       synonyms: [site, facility, op unit, governed op unit]
# MAGIC
# MAGIC     - name: fiscal_year
# MAGIC       expr: fiscal_year
# MAGIC       display_name: Fiscal Year
# MAGIC       comment: Fiscal year (starts Sep 1). FY2026 = Sep 2025 - Aug 2026.
# MAGIC       synonyms: [FY, year]
# MAGIC
# MAGIC     - name: fiscal_month
# MAGIC       expr: fiscal_month
# MAGIC       display_name: Fiscal Month
# MAGIC       comment: Fiscal month of year (1=Sep, 9=May, 12=Aug)
# MAGIC
# MAGIC     - name: month_name
# MAGIC       expr: month_name
# MAGIC       display_name: Month Name
# MAGIC
# MAGIC     - name: account_class
# MAGIC       expr: account_class_name
# MAGIC       display_name: Account Class
# MAGIC       comment: IP (Inpatient) or OBSV (Observation)
# MAGIC       synonyms: [encounter type, patient class, IP or OBSV]
# MAGIC
# MAGIC     - name: department
# MAGIC       expr: discharge_department_name
# MAGIC       display_name: Discharge Department
# MAGIC       synonyms: [unit, ward, discharge unit]
# MAGIC
# MAGIC     - name: payer
# MAGIC       expr: payer_category
# MAGIC       display_name: Payer Category
# MAGIC       comment: Financial class (Commercial, Medicare, Medicaid, etc.)
# MAGIC       synonyms: [financial class, insurance, coverage]
# MAGIC
# MAGIC     - name: discharge_date
# MAGIC       expr: discharge_date
# MAGIC       display_name: Discharge Date
# MAGIC       format:
# MAGIC         type: date
# MAGIC         date_format: year_month_day
# MAGIC
# MAGIC     - name: vizient_service_line
# MAGIC       expr: vizient_service_line
# MAGIC       display_name: Vizient Service Line
# MAGIC
# MAGIC     - name: primary_diagnosis
# MAGIC       expr: primary_dx_name
# MAGIC       display_name: Primary Diagnosis
# MAGIC       synonyms: [diagnosis, dx]
# MAGIC
# MAGIC   measures:
# MAGIC     - name: discharges
# MAGIC       expr: COUNT(hsp_account_id)
# MAGIC       display_name: Discharges
# MAGIC       comment: Total hospital discharges
# MAGIC       synonyms: [discharge count, volume, encounters]
# MAGIC       format:
# MAGIC         type: number
# MAGIC         decimal_places: { type: exact, places: 0 }
# MAGIC
# MAGIC     - name: avg_los_days
# MAGIC       expr: ROUND(AVG(los_observed_days), 2)
# MAGIC       display_name: Average LOS (Days)
# MAGIC       comment: Average observed LOS in fractional days (all IP & OBSV)
# MAGIC       synonyms: [average length of stay, mean LOS, avg LOS]
# MAGIC       format:
# MAGIC         type: number
# MAGIC         decimal_places: { type: exact, places: 2 }
# MAGIC
# MAGIC     - name: avg_inpatient_los_days
# MAGIC       expr: ROUND(AVG(los_inpatient_days), 2)
# MAGIC       display_name: Average Inpatient LOS (Days)
# MAGIC       comment: Average inpatient portion of LOS in days
# MAGIC       synonyms: [inpatient LOS, IP LOS]
# MAGIC       format:
# MAGIC         type: number
# MAGIC         decimal_places: { type: exact, places: 2 }
# MAGIC
# MAGIC     - name: avg_observation_los_hours
# MAGIC       expr: |-
# MAGIC         ROUND(AVG(CASE WHEN account_class_name = 'OBSV'
# MAGIC                        THEN los_observation_hours END), 2)
# MAGIC       display_name: Average Observation LOS (Hours)
# MAGIC       comment: Average observation LOS in hours (OBSV only)
# MAGIC       synonyms: [observation LOS, OBSV LOS]
# MAGIC       format:
# MAGIC         type: number
# MAGIC         decimal_places: { type: exact, places: 2 }
# MAGIC
# MAGIC     - name: long_los_rate
# MAGIC       expr: ROUND(AVG(long_los_flag) * 100, 1)
# MAGIC       display_name: Long LOS Rate (%)
# MAGIC       comment: Percent of encounters exceeding long LOS threshold
# MAGIC       synonyms: [long stay rate, extended stay rate]
# MAGIC       format:
# MAGIC         type: percentage
# MAGIC         decimal_places: { type: exact, places: 1 }
# MAGIC
# MAGIC     - name: avg_case_mix_index
# MAGIC       expr: ROUND(AVG(case_mix_index), 4)
# MAGIC       display_name: Average CMI
# MAGIC       synonyms: [CMI, case mix]
# MAGIC       format:
# MAGIC         type: number
# MAGIC         decimal_places: { type: exact, places: 4 }
# MAGIC
# MAGIC     - name: icu_encounter_rate
# MAGIC       expr: ROUND(AVG(icu_encounter_flag) * 100, 1)
# MAGIC       display_name: ICU Encounter Rate (%)
# MAGIC       synonyms: [ICU rate, critical care rate]
# MAGIC       format:
# MAGIC         type: percentage
# MAGIC         decimal_places: { type: exact, places: 1 }
# MAGIC
# MAGIC     - name: mortality_rate
# MAGIC       expr: ROUND(AVG(mortality_flag) * 100, 2)
# MAGIC       display_name: Mortality Rate (%)
# MAGIC       comment: In-hospital observed mortality rate
# MAGIC       format:
# MAGIC         type: percentage
# MAGIC         decimal_places: { type: exact, places: 2 }
# MAGIC $$;
# MAGIC
# MAGIC -- Tag the metric view for Discover page
# MAGIC ALTER VIEW ${catalog}.${schema_name}.los_metrics SET TAGS ('domain' = 'operations');

# COMMAND ----------

# DBTITLE 1,Validate the metric view
# MAGIC %sql
# MAGIC -- 🔍 Validate: Query the metric view with MEASURE() syntax
# MAGIC -- This is the same syntax Genie and dashboards will use
# MAGIC
# MAGIC SELECT hospital,
# MAGIC        MEASURE(discharges)             AS discharges,
# MAGIC        MEASURE(avg_los_days)           AS avg_los_days,
# MAGIC        MEASURE(avg_inpatient_los_days) AS avg_ip_los,
# MAGIC        MEASURE(long_los_rate)          AS long_los_pct,
# MAGIC        MEASURE(icu_encounter_rate)     AS icu_rate
# MAGIC FROM   ${catalog}.${schema_name}.los_metrics
# MAGIC WHERE  fiscal_year = 2026 AND fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY ALL
# MAGIC ORDER  BY discharges DESC;

# COMMAND ----------

# DBTITLE 1,Workstream 2 — SQL Expressions
# MAGIC %md
# MAGIC # Workstream 2 — SQL Expressions
# MAGIC ## Concept: Filters, measures, and fields with synonyms
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC SQL expressions are **pre-built calculations and filters** that you register in a Genie space so the AI uses your exact business logic. They're the bridge between business language and SQL.
# MAGIC
# MAGIC ### Three Types of SQL Expressions
# MAGIC
# MAGIC | Type | Purpose | Example |
# MAGIC |---|---|---|
# MAGIC | **Measures** | Pre-defined aggregations | `AVG(los_observed_days)` → "average length of stay" |
# MAGIC | **Filters** | Scoped subsets of data | `fiscal_year = 2026` → "FY2026" |
# MAGIC | **Fields with Synonyms** | Alias columns to business terms | `hospital` → "site", "facility", "op unit" |
# MAGIC
# MAGIC ### Why SQL Expressions Beat Free-Form AI
# MAGIC * **Reproducibility** — Same question always produces same SQL
# MAGIC * **Accuracy** — Business-approved formulas, not AI guesses
# MAGIC * **Discoverability** — Users see what metrics are available
# MAGIC * **Governance** — One source of truth for metric definitions
# MAGIC
# MAGIC ### LOS Expressions to Build
# MAGIC
# MAGIC **Measures:**
# MAGIC * `Discharges` → `COUNT(hsp_account_id)`
# MAGIC * `Average LOS (days)` → `ROUND(AVG(los_observed_days), 2)`
# MAGIC * `Average Inpatient LOS (days)` → `ROUND(AVG(los_inpatient_days), 2)`
# MAGIC * `Average Observation LOS (hours)` → `ROUND(AVG(los_observation_hours), 2)`
# MAGIC * `Long LOS Rate` → `ROUND(AVG(long_los_flag) * 100, 1)`
# MAGIC
# MAGIC **Filters:**
# MAGIC * `FY2026` → `fiscal_year = 2026`
# MAGIC * `FY2026 Q1-Q3` → `fiscal_year = 2026 AND fiscal_month BETWEEN 1 AND 9`
# MAGIC * `Inpatient Only` → `account_class_name = 'IP'`
# MAGIC * `Observation Only` → `account_class_name = 'OBSV'`
# MAGIC
# MAGIC **Synonyms:**
# MAGIC * `hospital` → site, facility, op unit, governed op unit
# MAGIC * `payer_category` → financial class, payer, insurance
# MAGIC * `discharge_department_name` → department, unit, ward
# MAGIC * `los_observed_days` → length of stay, LOS, days in hospital
# MAGIC
# MAGIC ### 💡 Pro Tips
# MAGIC * Define measures at the **Genie space level**, not in the view — this gives users natural-language access
# MAGIC * Use synonyms liberally — if a user might say it, add it
# MAGIC * Filters are most useful for **fiscal year/period** and **population slices**

# COMMAND ----------

# DBTITLE 1,Workstream 3 — Example SQL Queries
# MAGIC %md
# MAGIC # Workstream 3 — Example SQL Queries
# MAGIC ## Concept: Static + parameterized queries curated as trusted assets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC Example SQL queries are the **most powerful tuning lever** for a Genie space. They teach Genie exactly how to answer specific types of questions. When a user's question matches an example, Genie uses the curated SQL rather than generating its own.
# MAGIC
# MAGIC ### Two Types of Example Queries
# MAGIC
# MAGIC | Type | When to Use | Example |
# MAGIC |---|---|---|
# MAGIC | **Static** | Exact, frequently-asked questions | "How many discharges in FY2026?" |
# MAGIC | **Parameterized** | Template questions with variable inputs | "Average LOS by {dimension} in FY{year}" |
# MAGIC
# MAGIC ### Why Examples > Text Instructions
# MAGIC * SQL is **unambiguous** — text instructions are open to interpretation
# MAGIC * Examples **teach by showing**, not by telling
# MAGIC * Genie can **generalize** from examples to handle similar but not identical questions
# MAGIC * Examples are **testable** — you can verify the SQL produces the right answer
# MAGIC
# MAGIC ### ❗ Benchmark Questions ≠ Example Queries
# MAGIC
# MAGIC This is the most common mistake teams make: dumping all benchmark questions into the Genie space as example queries. **Don't do this.**
# MAGIC
# MAGIC | | Benchmark Questions | Example Queries (Genie Space) |
# MAGIC |---|---|---|
# MAGIC | **Purpose** | Validate the gold view and measure accuracy | Teach Genie SQL patterns it can generalize from |
# MAGIC | **Count** | As many as needed (14 for LOS) | **2–3 carefully chosen** |
# MAGIC | **Where they live** | This notebook (run → verify → score) | Registered in the Genie space as trusted assets |
# MAGIC | **SQL style** | Raw SQL on `gold_los` (for validation) | `MEASURE()` syntax on `los_metrics` (for the agent) |
# MAGIC
# MAGIC ### Why 2–3 beats 14
# MAGIC * Genie **generalizes** from examples — one query with `GROUP BY hospital` teaches it to group by any dimension
# MAGIC * Too many examples **over-constrain** the agent — it pattern-matches instead of reasoning
# MAGIC * Each example is a **maintenance burden** — fewer = easier to keep current
# MAGIC
# MAGIC ### How to Pick Your 2–3
# MAGIC Choose queries that **teach different patterns**, not different questions:
# MAGIC 1. **Basic measure + dimension** — teaches the standard `SELECT dimension, MEASURE(x) ... GROUP BY ALL` pattern
# MAGIC 2. **Multi-measure comparison or filter** — teaches combining measures or filtering to a population subset
# MAGIC 3. **Ranking or HAVING** — teaches `ORDER BY ... LIMIT` or `HAVING MEASURE(x) >= N` patterns
# MAGIC
# MAGIC ### ⭐ Recommended LOS Example Queries (for the Genie space)
# MAGIC
# MAGIC **E1 (Basic):** Average LOS by hospital
# MAGIC ```sql
# MAGIC SELECT hospital, MEASURE(avg_los_days) AS avg_los_days, MEASURE(discharges) AS discharges
# MAGIC FROM   los_metrics
# MAGIC WHERE  fiscal_year = 2026
# MAGIC GROUP  BY ALL
# MAGIC ORDER  BY avg_los_days DESC
# MAGIC ```
# MAGIC
# MAGIC **E2 (Segmentation):** IP vs OBSV comparison
# MAGIC ```sql
# MAGIC SELECT account_class, MEASURE(discharges) AS discharges, MEASURE(avg_los_days) AS avg_los_days
# MAGIC FROM   los_metrics
# MAGIC WHERE  fiscal_year = 2026
# MAGIC GROUP  BY ALL
# MAGIC ```
# MAGIC
# MAGIC **E3 (Ranking):** Top departments by LOS with volume threshold
# MAGIC ```sql
# MAGIC SELECT department, MEASURE(avg_los_days) AS avg_los_days, MEASURE(discharges) AS discharges
# MAGIC FROM   los_metrics
# MAGIC WHERE  fiscal_year = 2026
# MAGIC GROUP  BY ALL
# MAGIC HAVING MEASURE(discharges) >= 50
# MAGIC ORDER  BY avg_los_days DESC
# MAGIC LIMIT  10
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 LOS Benchmark Questions (14)
# MAGIC The following 14 questions validate the gold view. Run each, verify the answer, then score Genie against them in Workstream 6. **These stay in the notebook — only E1–E3 above go into the Genie space.**

# COMMAND ----------

# DBTITLE 1,Volume & Throughput
# MAGIC %md
# MAGIC ## 📊 Category 1: Volume & Throughput

# COMMAND ----------

# DBTITLE 1,Q1: How many discharges occurred during FY2026 Q1-Q3?
# MAGIC %sql
# MAGIC -- BENCHMARK Q1 (Static): Total discharges for FY2026 Q1-Q3
# MAGIC -- Benchmark question: "How many discharges occurred during FY2026 Q1-Q3?"
# MAGIC -- Validates against: LOS PBI > Metric Summary > All Hospital Accounts (IP & OBSV)
# MAGIC
# MAGIC SELECT COUNT(hsp_account_id) AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9;

# COMMAND ----------

# DBTITLE 1,Q2: Discharges by fiscal month during FY2026 Q1-Q3
# MAGIC %sql
# MAGIC -- BENCHMARK Q2 (Static): Discharges by fiscal month
# MAGIC -- Benchmark question: "How many discharges occurred by fiscal month during FY2026 Q1-Q3?"
# MAGIC
# MAGIC SELECT fiscal_month,
# MAGIC        month_name,
# MAGIC        COUNT(hsp_account_id) AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY fiscal_month, month_name
# MAGIC ORDER  BY fiscal_month;

# COMMAND ----------

# DBTITLE 1,Q3: Which hospitals had the highest discharge volumes?
# MAGIC %sql
# MAGIC -- BENCHMARK Q3 (Static): Discharges by hospital
# MAGIC -- Benchmark question: "Which hospitals had the highest discharge volumes during FY2026 Q1-Q3?"
# MAGIC
# MAGIC SELECT hospital,
# MAGIC        COUNT(hsp_account_id) AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY hospital
# MAGIC ORDER  BY discharges DESC;

# COMMAND ----------

# DBTITLE 1,Average Length of Stay
# MAGIC %md
# MAGIC ## ⏱️ Category 2: Average Length of Stay

# COMMAND ----------

# DBTITLE 1,Q4: Average LOS across all inpatient encounters
# MAGIC %sql
# MAGIC -- BENCHMARK Q4 (Static): Average LOS for all encounters
# MAGIC -- Benchmark question: "What was the average length of stay across all inpatient encounters during FY2026 Q1-Q3?"
# MAGIC -- Note: Uses inpatient_portion metric (matches PBI Metric 5)
# MAGIC
# MAGIC SELECT ROUND(AVG(los_inpatient_days), 2) AS avg_inpatient_los_days
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9;

# COMMAND ----------

# DBTITLE 1,Q5: Average LOS by hospital
# MAGIC %sql
# MAGIC -- BENCHMARK Q5 (Static): Average LOS by hospital
# MAGIC -- Benchmark question: "What was the average length of stay by hospital during FY2026 Q1-Q3?"
# MAGIC
# MAGIC SELECT hospital,
# MAGIC        ROUND(AVG(los_inpatient_days), 2) AS avg_inpatient_los_days,
# MAGIC        COUNT(hsp_account_id)             AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY hospital
# MAGIC ORDER  BY avg_inpatient_los_days DESC;

# COMMAND ----------

# DBTITLE 1,Q6: Average LOS by discharge department
# MAGIC %sql
# MAGIC -- BENCHMARK Q6 (Parameterized pattern): Average LOS by dimension
# MAGIC -- Benchmark question: "What was the average length of stay by discharge department during FY2026 Q1-Q3?"
# MAGIC -- This pattern generalizes to any dimension (hospital, payer, provider, etc.)
# MAGIC
# MAGIC SELECT discharge_department_name            AS department,
# MAGIC        ROUND(AVG(los_observed_days), 2)     AS avg_los_days,
# MAGIC        COUNT(hsp_account_id)                AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY discharge_department_name
# MAGIC ORDER  BY avg_los_days DESC;

# COMMAND ----------

# DBTITLE 1,Trends
# MAGIC %md
# MAGIC ## 📈 Category 3: Trends

# COMMAND ----------

# DBTITLE 1,Q7: Which fiscal month had the highest avg LOS?
# MAGIC %sql
# MAGIC -- BENCHMARK Q7 (Static): LOS trend by fiscal month
# MAGIC -- Benchmark question: "Which fiscal month had the highest average length of stay during FY2026 Q1-Q3?"
# MAGIC
# MAGIC SELECT fiscal_month,
# MAGIC        month_name,
# MAGIC        ROUND(AVG(los_observed_days), 2)  AS avg_los_days,
# MAGIC        COUNT(hsp_account_id)             AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY fiscal_month, month_name
# MAGIC ORDER  BY avg_los_days DESC;

# COMMAND ----------

# DBTITLE 1,Q8: Which fiscal month had the highest discharge volume?
# MAGIC %sql
# MAGIC -- BENCHMARK Q8 (Static): Discharge volume trend by fiscal month
# MAGIC -- Benchmark question: "Which fiscal month had the highest discharge volume during FY2026 Q1-Q3?"
# MAGIC
# MAGIC SELECT fiscal_month,
# MAGIC        month_name,
# MAGIC        COUNT(hsp_account_id) AS discharges
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY fiscal_month, month_name
# MAGIC ORDER  BY discharges DESC;

# COMMAND ----------

# DBTITLE 1,Segmentation
# MAGIC %md
# MAGIC ## 🧩 Category 4: Segmentation

# COMMAND ----------

# DBTITLE 1,Q9: LOS comparison between IP and OBSV encounters
# MAGIC %sql
# MAGIC -- BENCHMARK Q9 (Static): IP vs OBSV comparison
# MAGIC -- Benchmark question: "How does average length of stay compare between inpatient and observation encounters?"
# MAGIC -- Note: IP uses days metric, OBSV uses hours metric (matches PBI Metrics 5 and 7)
# MAGIC
# MAGIC SELECT
# MAGIC   ROUND(AVG(los_inpatient_days), 2)                                           AS avg_inpatient_portion_days,
# MAGIC   ROUND(AVG(CASE WHEN account_class_name = 'OBSV'
# MAGIC                  THEN los_observation_hours END), 2)                           AS avg_obsv_portion_hours,
# MAGIC   ROUND(AVG(CASE WHEN account_class_name = 'IP'
# MAGIC                  THEN los_observed_days END), 2)                               AS avg_los_ip_days,
# MAGIC   ROUND(AVG(CASE WHEN account_class_name = 'OBSV'
# MAGIC                  THEN los_observed_days END), 2)                               AS avg_los_obsv_days,
# MAGIC   COUNT(CASE WHEN account_class_name = 'IP'   THEN 1 END)                     AS discharges_ip,
# MAGIC   COUNT(CASE WHEN account_class_name = 'OBSV' THEN 1 END)                     AS discharges_obsv
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9;

# COMMAND ----------

# DBTITLE 1,Q10: Average LOS by payer category
# MAGIC %sql
# MAGIC -- BENCHMARK Q10 (Static): LOS by payer / financial class
# MAGIC -- Benchmark question: "What was the average length of stay by payer category?"
# MAGIC
# MAGIC SELECT payer_category,
# MAGIC        COUNT(hsp_account_id)              AS discharges,
# MAGIC        ROUND(AVG(los_observed_days), 2)   AS avg_los_days
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY payer_category
# MAGIC ORDER  BY avg_los_days DESC;

# COMMAND ----------

# DBTITLE 1,Ranking & Prioritization
# MAGIC %md
# MAGIC ## 🏆 Category 5: Ranking & Prioritization

# COMMAND ----------

# DBTITLE 1,Q11: Top 10 departments by average LOS
# MAGIC %sql
# MAGIC -- BENCHMARK Q11 (Static): Top 10 departments by avg LOS
# MAGIC -- Benchmark question: "Which ten departments had the highest average length of stay during FY2026 Q1-Q3?"
# MAGIC
# MAGIC SELECT discharge_department_name              AS department,
# MAGIC        COUNT(hsp_account_id)                  AS discharges,
# MAGIC        ROUND(AVG(los_observed_days), 2)       AS avg_los_days
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY discharge_department_name
# MAGIC ORDER  BY avg_los_days DESC
# MAGIC LIMIT  10;

# COMMAND ----------

# DBTITLE 1,Q12: YoY hospital LOS change (FY2026 vs FY2025)
# MAGIC %sql
# MAGIC -- BENCHMARK Q12 (Static): Year-over-year hospital comparison
# MAGIC -- Benchmark question: "Which hospitals experienced the largest increase in average LOS in FY2026 compared to FY2025?"
# MAGIC -- Compares FYTD: FY2026 FM 1-9 vs FY2025 FM 1-9
# MAGIC
# MAGIC WITH yearly AS (
# MAGIC   SELECT hospital,
# MAGIC          fiscal_year                     AS fy,
# MAGIC          COUNT(hsp_account_id)           AS discharges,
# MAGIC          AVG(los_observed_days)          AS avg_los
# MAGIC   FROM   ${catalog}.${schema_name}.gold_los
# MAGIC   WHERE  fiscal_year IN (2025, 2026)
# MAGIC     AND  fiscal_month BETWEEN 1 AND 9
# MAGIC   GROUP  BY hospital, fiscal_year
# MAGIC )
# MAGIC SELECT cur.hospital,
# MAGIC        ROUND(cur.avg_los, 2)                                     AS avg_los_fy2026,
# MAGIC        ROUND(prior.avg_los, 2)                                   AS avg_los_fy2025,
# MAGIC        ROUND(cur.avg_los - prior.avg_los, 2)                     AS los_change_days,
# MAGIC        ROUND((cur.avg_los - prior.avg_los) / prior.avg_los * 100, 1) AS los_change_pct,
# MAGIC        cur.discharges                                            AS discharges_fy2026,
# MAGIC        prior.discharges                                          AS discharges_fy2025
# MAGIC FROM   yearly cur
# MAGIC JOIN   yearly prior
# MAGIC   ON   cur.hospital = prior.hospital
# MAGIC   AND  cur.fy = 2026
# MAGIC   AND  prior.fy = 2025
# MAGIC ORDER  BY los_change_pct DESC;

# COMMAND ----------

# DBTITLE 1,Critical Thinking
# MAGIC %md
# MAGIC ## 🧠 Category 6: Critical Thinking
# MAGIC These questions test whether Genie can handle nuance and reproducibility — the hardest category.

# COMMAND ----------

# DBTITLE 1,Q13: Top 10 areas of LOS variation
# MAGIC %sql
# MAGIC -- BENCHMARK Q13 (Static): Areas with highest LOS variation
# MAGIC -- Benchmark question: "What are the 10 biggest areas of average length of stay variation using the full data set timeframe?"
# MAGIC -- Uses standard deviation as a measure of variation across departments
# MAGIC
# MAGIC SELECT discharge_department_name              AS department,
# MAGIC        COUNT(hsp_account_id)                  AS discharges,
# MAGIC        ROUND(AVG(los_observed_days), 2)       AS avg_los_days,
# MAGIC        ROUND(STDDEV(los_observed_days), 2)    AS stddev_los_days,
# MAGIC        ROUND(MIN(los_observed_days), 2)       AS min_los_days,
# MAGIC        ROUND(MAX(los_observed_days), 2)       AS max_los_days
# MAGIC FROM   ${catalog}.${schema_name}.gold_los
# MAGIC WHERE  fiscal_year = 2026
# MAGIC   AND  fiscal_month BETWEEN 1 AND 9
# MAGIC GROUP  BY discharge_department_name
# MAGIC HAVING COUNT(hsp_account_id) >= 50    -- Minimum volume for meaningful variation
# MAGIC ORDER  BY stddev_los_days DESC
# MAGIC LIMIT  10;

# COMMAND ----------

# DBTITLE 1,Q14: Reproducibility context for analysts
# MAGIC %md
# MAGIC ### Q14: What context would you provide a data analyst to ensure reproducible results?
# MAGIC
# MAGIC **This is a Genie text instruction question.** The answer is the metadata and business rules baked into the gold view and Genie space:
# MAGIC
# MAGIC 1. **Population filter:** `har_in_hd_dm_flag = 1` (All Hospital Discharges IP & OBSV)
# MAGIC 2. **Fiscal calendar:** FY starts September 1. FM 1 = September, FM 12 = August. Use `fiscal_year` and `fiscal_month` from the gold view, not calendar month.
# MAGIC 3. **LOS metric selection:**
# MAGIC    * Overall IP & OBSV → `los_observed_days` (fractional days)
# MAGIC    * Inpatient portion only → `los_inpatient_days`
# MAGIC    * Observation portion → `los_observation_hours` (hours, not days)
# MAGIC 4. **Rounding:** Dashboard uses `ROUND(AVG(...), 2)` for all LOS averages
# MAGIC 5. **Hospital ordering:** NMH, CDH, PH, MCH, LFH, KH, VH, DHG (by `hospital_sort`)
# MAGIC 6. **Data freshness:** T-1 (data is available through yesterday)
# MAGIC 7. **Vizient-qualified subset:** Use `vizient_los_denom_flag = 1` for Vizient-specific analyses
# MAGIC
# MAGIC **💡 This is exactly the kind of context that belongs in UC metadata comments and Genie text instructions.**

# COMMAND ----------

# DBTITLE 1,⭐ BUILD: Create Your Genie Space
# MAGIC %md
# MAGIC # ⭐ BUILD: Create Your Genie Space
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Steps to Create the LOS Genie Space
# MAGIC
# MAGIC 1. **Navigate to Genie** — In the left sidebar, click on **Genie** (under AI/BI)
# MAGIC 2. **Create New Space** — Click "+ New" and name it **"Length of Stay Agent"**
# MAGIC 3. **Add Tables** — Add the gold view: `clinical_dm.hospital_encounters.gold_los`
# MAGIC 4. **Add SQL Expressions** (from Workstream 2):
# MAGIC    * Measure: `Discharges` → `COUNT(hsp_account_id)`
# MAGIC    * Measure: `Average LOS (days)` → `ROUND(AVG(los_observed_days), 2)`
# MAGIC    * Measure: `Average Inpatient LOS (days)` → `ROUND(AVG(los_inpatient_days), 2)`
# MAGIC    * Filter: `FY2026` → `fiscal_year = 2026`
# MAGIC    * Filter: `FY2026 Q1-Q3` → `fiscal_year = 2026 AND fiscal_month BETWEEN 1 AND 9`
# MAGIC 5. **Add Example Queries** (from Workstream 3 — only 2–3!):
# MAGIC    * **E1:** Average LOS by hospital (teaches basic measure + dimension pattern)
# MAGIC    * **E2:** IP vs OBSV comparison (teaches segmentation / account_class filter)
# MAGIC    * **E3:** Top departments with volume threshold (teaches ranking + HAVING)
# MAGIC    * Use `MEASURE()` syntax on `los_metrics`, not raw SQL on the gold view
# MAGIC    * Do NOT add all 14 benchmark questions — Genie generalizes from patterns
# MAGIC 6. **Add Text Instructions** (from Workstream 4 — keep minimal!)
# MAGIC 7. **Test with Benchmark Questions** — Ask each of the 14 questions and validate
# MAGIC
# MAGIC ### 💡 Order of Operations Matters
# MAGIC Build in this order for best results:
# MAGIC ```
# MAGIC Gold view (data) → UC metadata (context) → SQL expressions (calculations) → Example queries (patterns) → Text instructions (last resort)
# MAGIC ```
# MAGIC Each layer reduces the need for the next. If your gold view and UC metadata are excellent, you'll need fewer SQL expressions. If your SQL expressions cover the metrics, you'll need fewer example queries. And so on.

# COMMAND ----------

# DBTITLE 1,Workstream 4 — Text Instructions
# MAGIC %md
# MAGIC # Workstream 4 — Text Instructions
# MAGIC ## Concept: Last resort — keep minimal and specific
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC Text instructions are the **last tuning lever**, not the first. If you find yourself writing a paragraph of instructions, you probably need a better SQL expression or example query instead.
# MAGIC
# MAGIC ### When to Use Text Instructions
# MAGIC * **Domain rules** that can't be expressed in SQL (e.g., "the fiscal year starts in September")
# MAGIC * **Disambiguation** (e.g., "when a user says 'LOS' they mean length of stay in days, not the city")
# MAGIC * **Default behavior** (e.g., "unless specified, default to the current fiscal year")
# MAGIC * **Column preference** (e.g., "for average LOS, use `los_observed_days` unless the user specifies inpatient or observation")
# MAGIC
# MAGIC ### When NOT to Use Text Instructions
# MAGIC * Metric definitions → Use **SQL expressions** instead
# MAGIC * How to join tables → Use a **denormalized gold view** instead
# MAGIC * Common query patterns → Use **example SQL queries** instead
# MAGIC * Filter logic → Use **SQL expression filters** instead
# MAGIC
# MAGIC ### ⭐ BUILD: Recommended Text Instructions for the LOS Space
# MAGIC
# MAGIC ```
# MAGIC 1. The fiscal year runs September through August. Fiscal year 2026 starts September 1, 2025. Fiscal month 1 = September, fiscal month 9 = May, fiscal month 12 = August.
# MAGIC
# MAGIC 2. When the user asks about "average LOS" or "length of stay" without specifying inpatient or observation, use los_observed_days (fractional days for all IP & OBSV encounters).
# MAGIC
# MAGIC 3. When the user asks about "inpatient LOS", use los_inpatient_days. When they ask about "observation LOS", use los_observation_hours (in hours, not days).
# MAGIC
# MAGIC 4. Hospital abbreviations: NMH = Northwestern Memorial Hospital, CDH = Central DuPage Hospital, PH = Palos Hospital, MCH = Marianjoy, LFH = Lake Forest Hospital, KH = Kishwaukee Hospital, VH = Valley West Hospital, DHG = Delnor Hospital.
# MAGIC
# MAGIC 5. Always round LOS averages to 2 decimal places.
# MAGIC
# MAGIC 6. The data refreshes daily and contains data through yesterday (T-1).
# MAGIC
# MAGIC 7. Unless the user specifies a time period, default to the current fiscal year to date.
# MAGIC ```
# MAGIC
# MAGIC ### 💡 Notice how minimal this is
# MAGIC * 7 short rules, not 70
# MAGIC * Each addresses a specific ambiguity
# MAGIC * Most of the "instructions" are actually UC column comments or SQL expressions

# COMMAND ----------

# DBTITLE 1,Workstream 5 — AI/BI Dashboard Configuration
# MAGIC %md
# MAGIC # Workstream 5 — AI/BI Dashboard Configuration
# MAGIC ## Concept: Wire curated metrics into governed dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC AI/BI dashboards are the **visual layer** on top of your Genie space. They share the same data, metrics, and governance model, giving users a familiar BI experience alongside natural-language Q&A.
# MAGIC
# MAGIC ### Dashboard ↔ Genie Space Relationship
# MAGIC * Both use the **same gold view** as their data source
# MAGIC * Dashboard widgets should mirror the **same metrics** defined in your SQL expressions
# MAGIC * Users can click "Ask Genie" from a dashboard to drill deeper — the space is already scoped
# MAGIC
# MAGIC ### ⭐ BUILD: LOS Dashboard Widgets to Create
# MAGIC
# MAGIC Recommended layout for the LOS dashboard:
# MAGIC
# MAGIC | Widget | Type | Metric |
# MAGIC |---|---|---|
# MAGIC | Total Discharges | Counter | `COUNT(hsp_account_id)` |
# MAGIC | Average LOS (days) | Counter | `ROUND(AVG(los_observed_days), 2)` |
# MAGIC | Long LOS Rate | Counter | `ROUND(AVG(long_los_flag) * 100, 1)` |
# MAGIC | Discharges by Month | Bar chart | `fiscal_month` x `COUNT(*)` |
# MAGIC | Avg LOS by Hospital | Bar chart | `hospital` x `AVG(los_observed_days)` |
# MAGIC | LOS Trend by Month | Line chart | `fiscal_month` x `AVG(los_observed_days)` |
# MAGIC | IP vs OBSV Comparison | Bar chart | `account_class_name` split |
# MAGIC | Top 10 Departments | Table | Ranked by `AVG(los_observed_days)` |
# MAGIC | LOS by Payer | Bar chart | `payer_category` x `AVG(los_observed_days)` |
# MAGIC
# MAGIC ### 💡 Exercise
# MAGIC Create an AI/BI dashboard that mirrors the key views from the existing PBI LOS dashboard. Use the gold view as your data source.

# COMMAND ----------

# DBTITLE 1,Workstream 6 — Benchmarks, Review & Monitoring
# MAGIC %md
# MAGIC # Workstream 6 — Benchmarks, Review & Monitoring
# MAGIC ## Concept: Benchmark suite + weekly digest feedback loop
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC A Genie space is **never done**. You need a structured process to measure accuracy, collect feedback, and iterate. The benchmark suite is your regression test.
# MAGIC
# MAGIC ### The Benchmark Workflow
# MAGIC
# MAGIC ```
# MAGIC 1. Define benchmark questions (done! — 14 questions from the LOS team)
# MAGIC 2. Record expected answers (from PBI dashboard validation)
# MAGIC 3. Ask Genie each question and compare to expected answer
# MAGIC 4. Score: correct, partially correct, or wrong
# MAGIC 5. Fix: add/refine SQL expressions, example queries, or text instructions
# MAGIC 6. Re-run benchmarks
# MAGIC 7. Repeat weekly with user feedback
# MAGIC ```
# MAGIC
# MAGIC ### Benchmark Scorecard Template
# MAGIC
# MAGIC | # | Question | Expected Answer | Genie Answer | Score | Fix Needed |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 1 | How many discharges in FY2026 Q1-Q3? | (from PBI) | (test live) | ✅/⚠️/❌ | |
# MAGIC | 2 | Discharges by fiscal month? | (from PBI) | (test live) | | |
# MAGIC | 3 | Highest volume hospital? | (from PBI) | (test live) | | |
# MAGIC | ... | ... | ... | ... | ... | ... |
# MAGIC
# MAGIC ### Monitoring After Launch
# MAGIC * **Weekly digest:** Review the top 20 questions users asked. Did Genie answer correctly?
# MAGIC * **Feedback loop:** Users can thumbs-up/down answers. Review thumbs-down weekly.
# MAGIC * **New questions:** If users repeatedly ask something Genie can't answer, add a new example query.
# MAGIC * **Data drift:** If source tables change schema, update the gold view and re-run benchmarks.
# MAGIC
# MAGIC ### ⭐ BUILD Exercise
# MAGIC 1. Open your Genie space
# MAGIC 2. Ask each of the 14 benchmark questions
# MAGIC 3. Compare answers to the validated results in the Dashboard Validation notebook
# MAGIC 4. Log scores in the scorecard
# MAGIC 5. Identify the top 3 gaps and fix them

# COMMAND ----------

# DBTITLE 1,Workstream 7 — Data Access Model
# MAGIC %md
# MAGIC # Workstream 7 — Data Access Model
# MAGIC ## Concept: Security, row/column controls, and space permissions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC Genie inherits Unity Catalog's security model. Data access is governed at the **table/view level**, not the Genie space level. The space permissions control who can **ask questions**, while UC controls what **data they can see**.
# MAGIC
# MAGIC ### Three Layers of Access Control
# MAGIC
# MAGIC | Layer | Controls | Managed By |
# MAGIC |---|---|---|
# MAGIC | **Unity Catalog** | Who can SELECT from the gold view | Data Engineer (GRANT/REVOKE) |
# MAGIC | **Row-Level Security** | Which rows a user sees (e.g., only their hospital) | Data Engineer (row filters) |
# MAGIC | **Genie Space Permissions** | Who can access the Genie space | Data Analyst (space settings) |
# MAGIC
# MAGIC ### LOS Access Considerations
# MAGIC * **PHI/PII columns:** The gold view includes `mrn`, `full_name`, `birth_date` (from the fact table). Consider whether these should be in the Genie-facing view or masked.
# MAGIC * **Hospital-level access:** Some users may only need data for their hospital. Row filters on `hospital` can enforce this.
# MAGIC * **Vizient data:** Vizient-qualified subset has additional sensitivity. Consider a separate view for Vizient-only analyses.
# MAGIC
# MAGIC ### ⭐ BUILD: Security Checklist
# MAGIC
# MAGIC - [ ] Review gold view columns for PHI/PII — remove or mask as needed
# MAGIC - [ ] Decide: does the Genie space need row-level security by hospital?
# MAGIC - [ ] Grant `SELECT` on the gold view to the Genie space service principal
# MAGIC - [ ] Set Genie space permissions: who can ask questions?
# MAGIC - [ ] Set Genie space permissions: who can manage/edit the space?
# MAGIC - [ ] Document the access model for the team
# MAGIC
# MAGIC ### 💡 Note on the Gold View
# MAGIC The gold view we built in Workstream 1 intentionally **excluded** PHI columns (MRN, patient name, birth date) to keep the Genie space safe for broad access. If specific users need these, create a second view with additional access controls.

# COMMAND ----------

# DBTITLE 1,Workstream 8 — Genie One
# MAGIC %md
# MAGIC # Workstream 8 — Genie One
# MAGIC ## Concept: Surface approved spaces to end users
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Principle
# MAGIC Genie One is the **single entry point** where end users discover and access approved Genie spaces. Instead of sharing space URLs, users go to one place and find the spaces relevant to them.
# MAGIC
# MAGIC ### How Genie One Works
# MAGIC 1. **Admins curate** which Genie spaces appear in Genie One
# MAGIC 2. **Users browse** available spaces by domain/topic
# MAGIC 3. **Permissions** are inherited — users only see spaces they have access to
# MAGIC 4. **One URL** to share with the organization: the Genie One landing page
# MAGIC
# MAGIC ### Publishing the LOS Space
# MAGIC
# MAGIC Once your Genie space passes benchmarks:
# MAGIC 1. Open the Genie space settings
# MAGIC 2. Enable "Publish to Genie One"
# MAGIC 3. Add a clear **title** and **description** so users can discover it:
# MAGIC    * Title: "Length of Stay Agent"
# MAGIC    * Description: "Analyze hospital length of stay across hospital sites. Covers discharges, average LOS, trends, payer analysis, and department rankings for Inpatient and Observation encounters."
# MAGIC 4. Set the **icon** and **category** for easy discovery
# MAGIC
# MAGIC ### ⭐ BUILD: Go-Live Checklist
# MAGIC
# MAGIC - [ ] All 14 benchmark questions return correct answers
# MAGIC - [ ] Gold view has complete UC metadata (comments on all key columns)
# MAGIC - [ ] SQL expressions defined for core measures and filters
# MAGIC - [ ] Example queries registered for all benchmark questions
# MAGIC - [ ] Text instructions are minimal (≤ 10 rules)
# MAGIC - [ ] AI/BI dashboard created and linked
# MAGIC - [ ] Access model documented and implemented
# MAGIC - [ ] Space published to Genie One
# MAGIC - [ ] Monitoring/feedback process established
# MAGIC - [ ] Training materials shared with end users

# COMMAND ----------

# DBTITLE 1,Day 2 — Present Your Space
# MAGIC %md
# MAGIC # 🏁 Day 2 Wrap-Up: Present Your Genie Space
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Demo Format (30 minutes)
# MAGIC Each team presents their Genie space to the group:
# MAGIC
# MAGIC 1. **Show 3-5 benchmark questions** — ask them live and show the answers
# MAGIC 2. **Walk through your trusted assets:**
# MAGIC    * Gold view: what did you denormalize and why?
# MAGIC    * SQL expressions: which measures and filters did you define?
# MAGIC    * Example queries: how many, and how did you choose them?
# MAGIC    * Text instructions: what (minimal) instructions did you add?
# MAGIC 3. **Share your benchmark scorecard** — what's your accuracy rate?
# MAGIC 4. **Identify next steps** — what gaps remain?
# MAGIC
# MAGIC ### Peer Review Checklist
# MAGIC As you watch other presentations, evaluate:
# MAGIC - [ ] Is the domain clearly scoped (one topic, not everything)?
# MAGIC - [ ] Does the gold view have complete UC metadata?
# MAGIC - [ ] Are SQL expressions used before text instructions?
# MAGIC - [ ] Do example queries cover the most common questions?
# MAGIC - [ ] Is the access model appropriate for the data sensitivity?
# MAGIC
# MAGIC ### 🚀 What's Next After the Workshop
# MAGIC * Run benchmarks weekly with real user questions
# MAGIC * Review Genie usage/feedback in the weekly digest
# MAGIC * Add new example queries as users surface new question patterns
# MAGIC * Iterate on text instructions — remove any that become unnecessary as you add better SQL expressions
# MAGIC * Consider expanding to other domains (Mortality, Readmissions, etc.)