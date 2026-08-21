# Databricks notebook source
# DBTITLE 1,Gold Layer - Comprehensive Business Metrics
# MAGIC %md
# MAGIC # Gold Layer: Comprehensive Business Metrics
# MAGIC
# MAGIC ## Professional Pattern: Multi-Entity Gold Layer
# MAGIC
# MAGIC **Bronze → Silver → Gold**
# MAGIC - Bronze: Raw ingestion (schema-on-read)
# MAGIC - Silver: Cleaned, typed, deduplicated
# MAGIC - **Gold: Business-ready aggregates and metrics**
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - `healthcare.silver.patients` (demographics & financial)
# MAGIC - `healthcare.silver.observations` (clinical measurements)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Gold Layer Tables
# MAGIC
# MAGIC This notebook creates **8 Gold tables** combining patient and observation data:
# MAGIC
# MAGIC ### 1. **patient_demographics_summary**
# MAGIC - Dimensional/lookup table
# MAGIC - Patient counts by demographics (gender, race, ethnicity, marital status)
# MAGIC - Geography-based breakdowns (state, county)
# MAGIC - **Use case:** Population health analysis, demographic reporting
# MAGIC
# MAGIC ### 2. **healthcare_financial_metrics**
# MAGIC - Fact/KPI table
# MAGIC - Average expenses, income, coverage by demographic segments
# MAGIC - Cost variation by geography
# MAGIC - **Use case:** Financial dashboards, budget planning
# MAGIC
# MAGIC ### 3. **patient_lifetime_analytics**
# MAGIC - Aggregate/analytical table
# MAGIC - Age calculations, lifespan statistics
# MAGIC - Living vs deceased patient metrics
# MAGIC - **Use case:** Actuarial analysis, population trends
# MAGIC
# MAGIC ### 4. **data_quality_metrics**
# MAGIC - Operational metrics table
# MAGIC - Completeness, quality flags, processing stats
# MAGIC - **Use case:** Data governance dashboards, SLA monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Professional Exam Topics Covered
# MAGIC
# MAGIC ✅ **Dimensional modeling** (facts vs dimensions)  
# MAGIC ✅ **Business metric design** (KPIs for BI/dashboards)  
# MAGIC ✅ **Aggregation patterns** (GROUP BY, window functions)  
# MAGIC ✅ **Query optimization** (pre-aggregated for fast queries)  
# MAGIC ✅ **Incremental patterns** (foundation for incremental refresh)  
# MAGIC ✅ **Partitioning strategy** (for large-scale Gold tables)
# MAGIC
# MAGIC **Source:** `healthcare.silver.patients` (113 unique patients)

# COMMAND ----------

# DBTITLE 1,Setup: Load Silver Tables
# Setup and Configuration
# Import libraries and load BOTH Silver tables

from pyspark.sql import functions as F
from pyspark.sql import Window

# Configuration
PATIENT_SILVER = "healthcare.silver.patients"
OBSERVATION_SILVER = "healthcare.silver.observations"
GOLD_SCHEMA = "healthcare.gold"

# Read Silver layers
df_patients = spark.table(PATIENT_SILVER)
df_observations = spark.table(OBSERVATION_SILVER)

patient_count = df_patients.count()
observation_count = df_observations.count()

print("="*60)
print("🏥 GOLD LAYER: COMPREHENSIVE BUSINESS METRICS")
print("="*60)
print(f"\n📊 Source Tables:")
print(f"   - Patients: {PATIENT_SILVER} ({patient_count:,} records)")
print(f"   - Observations: {OBSERVATION_SILVER} ({observation_count:,} records)")
print(f"\n🎯 Target Schema: {GOLD_SCHEMA}")
print(f"\n✅ Configuration loaded successfully")

# COMMAND ----------

# DBTITLE 1,Cell 2: Create Gold Schema
# MAGIC %sql
# MAGIC -- Cell 2: Create Gold Schema
# MAGIC -- Create the gold schema if it doesn't exist
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.gold
# MAGIC COMMENT 'Gold layer: Business-ready aggregates and metrics for analytics and BI';

# COMMAND ----------

# DBTITLE 1,Patient Table 1: Demographics Summary
# Patient Table 1: Demographics Summary
# Dimensional table: Patient counts by demographic attributes

# Aggregate by demographics
demographics_summary = df_patients.groupBy(
    "GENDER",
    "RACE",
    "ETHNICITY",
    "MARITAL",
    "STATE",
    "COUNTY"
).agg(
    F.count("Id").alias("patient_count"),
    F.countDistinct("Id").alias("unique_patients"),
    F.avg("INCOME").alias("avg_income"),
    F.min("BIRTHDATE").alias("oldest_birthdate"),
    F.max("BIRTHDATE").alias("youngest_birthdate"),
    F.sum(F.when(F.col("DEATHDATE").isNotNull(), 1).otherwise(0)).alias("deceased_count")
).withColumn(
    "processing_date",
    F.current_date()
).withColumn(
    "snapshot_timestamp",
    F.current_timestamp()
)

# Write to Gold
demographics_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.patient_demographics_summary")

print("✅ Gold table created: patient_demographics_summary")
print(f"   Rows: {demographics_summary.count()}")
print(f"   Grain: GENDER × RACE × ETHNICITY × MARITAL × STATE × COUNTY")

# COMMAND ----------

# DBTITLE 1,Patient Table 2: Healthcare Financial Metrics
# Patient Table 2: Healthcare Financial Metrics
# Fact table: Financial KPIs for business analytics

# Calculate financial metrics by demographic segments
financial_metrics = df_patients.groupBy(
    "GENDER",
    "STATE"
).agg(
    # Patient counts
    F.count("Id").alias("patient_count"),
    
    # Income metrics
    F.avg("INCOME").alias("avg_income"),
    F.min("INCOME").alias("min_income"),
    F.max("INCOME").alias("max_income"),
    F.stddev("INCOME").alias("stddev_income"),
    
    # Healthcare expense metrics
    F.avg("HEALTHCARE_EXPENSES").alias("avg_healthcare_expenses"),
    F.sum("HEALTHCARE_EXPENSES").alias("total_healthcare_expenses"),
    F.min("HEALTHCARE_EXPENSES").alias("min_healthcare_expenses"),
    F.max("HEALTHCARE_EXPENSES").alias("max_healthcare_expenses"),
    
    # Coverage metrics
    F.avg("HEALTHCARE_COVERAGE").alias("avg_healthcare_coverage"),
    F.sum("HEALTHCARE_COVERAGE").alias("total_healthcare_coverage"),
    
    # Quality metrics
    F.sum(F.when(F.col("data_quality_flag") == 1, 1).otherwise(0)).alias("quality_issues_count")
).withColumn(
    # Calculate expense-to-income ratio (business metric)
    "expense_to_income_ratio",
    F.round(F.col("avg_healthcare_expenses") / F.col("avg_income"), 4)
).withColumn(
    # Calculate coverage gap (business metric)
    "coverage_gap",
    F.round(F.col("avg_healthcare_expenses") - F.col("avg_healthcare_coverage"), 2)
).withColumn(
    "processing_date",
    F.current_date()
).withColumn(
    "snapshot_timestamp",
    F.current_timestamp()
)

# Write to Gold
financial_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.healthcare_financial_metrics")

print("✅ Gold table created: healthcare_financial_metrics")
print(f"   Rows: {financial_metrics.count()}")
print(f"   Grain: GENDER × STATE")
print(f"   Business metrics: expense_to_income_ratio, coverage_gap")

# COMMAND ----------

# DBTITLE 1,Patient Table 3: Patient Lifetime Analytics
# Patient Table 3: Patient Lifetime Analytics
# Analytical table: Age, lifespan, and mortality statistics

# Calculate age and lifespan metrics
lifetime_analytics = df_patients.withColumn(
    # Calculate current age (for living patients)
    "current_age",
    F.when(
        F.col("DEATHDATE").isNull(),
        F.floor(F.months_between(F.current_date(), F.col("BIRTHDATE")) / 12)
    ).otherwise(None)
).withColumn(
    # Calculate age at death (for deceased patients)
    "age_at_death",
    F.when(
        F.col("DEATHDATE").isNotNull(),
        F.floor(F.months_between(F.col("DEATHDATE"), F.col("BIRTHDATE")) / 12)
    ).otherwise(None)
).withColumn(
    # Categorize patient status
    "patient_status",
    F.when(F.col("DEATHDATE").isNotNull(), "deceased").otherwise("living")
)

# Aggregate by demographics
lifetime_summary = lifetime_analytics.groupBy(
    "GENDER",
    "RACE",
    "patient_status"
).agg(
    F.count("Id").alias("patient_count"),
    
    # Age statistics (living patients)
    F.avg("current_age").alias("avg_current_age"),
    F.min("current_age").alias("min_current_age"),
    F.max("current_age").alias("max_current_age"),
    
    # Lifespan statistics (deceased patients)
    F.avg("age_at_death").alias("avg_age_at_death"),
    F.min("age_at_death").alias("min_age_at_death"),
    F.max("age_at_death").alias("max_age_at_death"),
    F.stddev("age_at_death").alias("stddev_age_at_death")
).withColumn(
    "processing_date",
    F.current_date()
).withColumn(
    "snapshot_timestamp",
    F.current_timestamp()
)

# Write to Gold
lifetime_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.patient_lifetime_analytics")

print("✅ Gold table created: patient_lifetime_analytics")
print(f"   Rows: {lifetime_summary.count()}")
print(f"   Grain: GENDER × RACE × patient_status")
print(f"   Metrics: age statistics, lifespan statistics")

# COMMAND ----------

# DBTITLE 1,Patient Table 4: Data Quality Metrics
# Patient Table 4: Data Quality Metrics
# Operational table: Data quality and completeness tracking

# Calculate completeness and quality metrics
quality_metrics = df_patients.agg(
    # Totals
    F.count("*").alias("total_records"),
    F.countDistinct("Id").alias("unique_patients"),
    
    # Quality flags
    F.sum(F.when(F.col("data_quality_flag") == 1, 1).otherwise(0)).alias("quality_issues"),
    F.sum(F.when(F.col("data_quality_flag") == 0, 1).otherwise(0)).alias("clean_records"),
    
    # Completeness - Demographics
    F.sum(F.when(F.col("BIRTHDATE").isNull(), 1).otherwise(0)).alias("missing_birthdate"),
    F.sum(F.when(F.col("GENDER").isNull(), 1).otherwise(0)).alias("missing_gender"),
    F.sum(F.when(F.col("RACE").isNull(), 1).otherwise(0)).alias("missing_race"),
    
    # Completeness - Financial
    F.sum(F.when(F.col("INCOME").isNull(), 1).otherwise(0)).alias("missing_income"),
    F.sum(F.when(F.col("HEALTHCARE_EXPENSES").isNull(), 1).otherwise(0)).alias("missing_expenses"),
    F.sum(F.when(F.col("HEALTHCARE_COVERAGE").isNull(), 1).otherwise(0)).alias("missing_coverage"),
    
    # Completeness - Location
    F.sum(F.when(F.col("LAT").isNull(), 1).otherwise(0)).alias("missing_lat"),
    F.sum(F.when(F.col("LON").isNull(), 1).otherwise(0)).alias("missing_lon"),
    
    # Processing metadata
    F.min("processing_timestamp").alias("earliest_processing_timestamp"),
    F.max("processing_timestamp").alias("latest_processing_timestamp")
).withColumn(
    "measurement_date",
    F.current_date()
).withColumn(
    "measurement_timestamp",
    F.current_timestamp()
).withColumn(
    # Calculate overall completeness score (0-100%)
    "overall_completeness_pct",
    F.round(
        (F.col("clean_records") / F.col("total_records") * 100),
        2
    )
)

# Write to Gold
quality_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.data_quality_metrics")

print("✅ Gold table created: data_quality_metrics")
print(f"   Rows: {quality_metrics.count()} (single summary row)")
print(f"   Grain: Overall dataset metrics")
print(f"   Purpose: Data governance, SLA monitoring")

# COMMAND ----------

# DBTITLE 1,Clinical Table 1: Patient Clinical Summary
# Clinical Table 1: Patient Clinical Summary
# Join patients with their observation activity

patient_clinical_summary = (
    df_observations
    .groupBy("patient_id")
    .agg(
        F.count("*").alias("total_observations"),
        F.countDistinct("observation_code").alias("unique_observation_types"),
        F.countDistinct("observation_type").alias("unique_observation_categories"),
        F.min("observation_date").alias("first_observation_date"),
        F.max("observation_date").alias("last_observation_date"),
        F.datediff(F.max("observation_date"), F.min("observation_date")).alias("observation_span_days"),
        F.sum(F.when(F.col("observation_type") == "numeric", 1).otherwise(0)).alias("numeric_observations"),
        F.sum(F.when(F.col("observation_type") == "text", 1).otherwise(0)).alias("text_observations"),
        F.sum(F.when(F.col("category") == "vital-signs", 1).otherwise(0)).alias("vital_sign_observations"),
        F.sum("has_quality_issue").alias("observation_quality_issues")
    )
    .join(
        df_patients.select(
            F.col("Id").alias("patient_id"),
            "GENDER", "RACE", "ETHNICITY", "STATE", "COUNTY",
            "BIRTHDATE", "DEATHDATE",
            F.floor(F.months_between(F.current_date(), F.col("BIRTHDATE")) / 12).alias("current_age")
        ),
        "patient_id",
        "left"
    )
    .withColumn(
        "observations_per_year",
        F.when(
            F.col("observation_span_days") > 0,
            F.round(F.col("total_observations") / (F.col("observation_span_days") / 365.25), 2)
        ).otherwise(F.col("total_observations"))
    )
    .withColumn(
        "patient_status",
        F.when(F.col("DEATHDATE").isNull(), "Living").otherwise("Deceased")
    )
)

# Write to Gold
patient_clinical_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.patient_clinical_summary")

print("✅ CLINICAL TABLE 1: patient_clinical_summary")
print(f"Records: {patient_clinical_summary.count():,}")

# COMMAND ----------

# DBTITLE 1,Clinical Table 2: Vital Signs Analytics
# Clinical Table 2: Vital Signs Analytics
# Aggregate vital signs by demographics for population health insights

vital_signs_analytics = (
    df_observations
    .filter(F.col("category") == "vital-signs")
    .filter(F.col("observation_value").isNotNull())
    .join(
        df_patients.select(
            F.col("Id").alias("patient_id"),
            "GENDER", "RACE", "STATE",
            F.floor(F.months_between(F.current_date(), F.col("BIRTHDATE")) / 12).alias("age")
        ),
        "patient_id",
        "inner"
    )
    .withColumn(
        "age_group",
        F.when(F.col("age") < 18, "0-17")
        .when(F.col("age") < 35, "18-34")
        .when(F.col("age") < 50, "35-49")
        .when(F.col("age") < 65, "50-64")
        .otherwise("65+")
    )
    .groupBy("GENDER", "age_group", "observation_code", "observation_description", "units")
    .agg(
        F.count("*").alias("observation_count"),
        F.countDistinct("patient_id").alias("patient_count"),
        F.round(F.avg("observation_value"), 2).alias("avg_value"),
        F.round(F.min("observation_value"), 2).alias("min_value"),
        F.round(F.max("observation_value"), 2).alias("max_value"),
        F.round(F.stddev("observation_value"), 2).alias("stddev_value"),
        F.round(F.percentile_approx("observation_value", 0.25), 2).alias("p25_value"),
        F.round(F.percentile_approx("observation_value", 0.50), 2).alias("median_value"),
        F.round(F.percentile_approx("observation_value", 0.75), 2).alias("p75_value")
    )
)

# Write to Gold
vital_signs_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.vital_signs_analytics")

print("✅ CLINICAL TABLE 2: vital_signs_analytics")
print(f"Records: {vital_signs_analytics.count():,}")

# COMMAND ----------

# DBTITLE 1,Clinical Table 3: Observation Type Distribution
# Clinical Table 3: Observation Type Distribution
# What clinical measurements are being tracked and how often

observation_type_distribution = (
    df_observations
    .groupBy("category", "observation_code", "observation_description", "observation_type", "units")
    .agg(
        F.count("*").alias("total_observations"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.min("observation_date").alias("first_recorded"),
        F.max("observation_date").alias("last_recorded"),
        F.round(F.avg("observation_value"), 2).alias("avg_value"),
        F.sum(F.when(F.col("observation_value").isNotNull(), 1).otherwise(0)).alias("non_null_values")
    )
    .withColumn(
        "observations_per_patient",
        F.round(F.col("total_observations") / F.col("unique_patients"), 1)
    )
    .withColumn(
        "patient_coverage_pct",
        F.round((F.col("unique_patients") / F.lit(patient_count)) * 100, 1)
    )
    .withColumn(
        "value_completeness_pct",
        F.round((F.col("non_null_values") / F.col("total_observations")) * 100, 1)
    )
    .orderBy(F.desc("total_observations"))
)

# Write to Gold
observation_type_distribution.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.observation_type_distribution")

print("✅ CLINICAL TABLE 3: observation_type_distribution")
print(f"Records: {observation_type_distribution.count():,}")

# COMMAND ----------

# DBTITLE 1,Clinical Table 4: Clinical Risk Indicators
# Clinical Table 4: Clinical Risk Indicators
# Identify patients with abnormal vital signs or clinical risk factors

clinical_risk_indicators = (
    df_observations
    .filter(F.col("observation_value").isNotNull())
    .join(
        df_patients.select(
            F.col("Id").alias("patient_id"),
            "GENDER", "RACE", "STATE", "BIRTHDATE",
            F.floor(F.months_between(F.current_date(), F.col("BIRTHDATE")) / 12).alias("age")
        ),
        "patient_id",
        "inner"
    )
    .withColumn(
        "risk_flag",
        # BMI risk: <18.5 (underweight) or >30 (obese)
        F.when(
            (F.col("observation_code") == "39156-5") & 
            ((F.col("observation_value") < 18.5) | (F.col("observation_value") > 30)),
            "BMI Abnormal"
        )
        # Systolic BP: <90 or >140 (hypertension)
        .when(
            (F.col("observation_code") == "8480-6") & 
            ((F.col("observation_value") < 90) | (F.col("observation_value") > 140)),
            "Blood Pressure High/Low"
        )
        # Diastolic BP: <60 or >90
        .when(
            (F.col("observation_code") == "8462-4") & 
            ((F.col("observation_value") < 60) | (F.col("observation_value") > 90)),
            "Blood Pressure High/Low"
        )
        # Heart Rate: <60 or >100 (at rest)
        .when(
            (F.col("observation_code") == "8867-4") & 
            ((F.col("observation_value") < 60) | (F.col("observation_value") > 100)),
            "Heart Rate Abnormal"
        )
        # Body Temperature: <97 or >99.5 F
        .when(
            (F.col("observation_code") == "8310-5") & 
            ((F.col("observation_value") < 97) | (F.col("observation_value") > 99.5)),
            "Temperature Abnormal"
        )
        .otherwise(None)
    )
    .filter(F.col("risk_flag").isNotNull())
    .groupBy(
        "patient_id", "GENDER", "age", "RACE", "STATE", 
        "risk_flag", "observation_code", "observation_description"
    )
    .agg(
        F.count("*").alias("risk_occurrence_count"),
        F.round(F.avg("observation_value"), 2).alias("avg_risk_value"),
        F.round(F.min("observation_value"), 2).alias("min_value"),
        F.round(F.max("observation_value"), 2).alias("max_value"),
        F.max("observation_date").alias("last_risk_observation_date"),
        F.first("units").alias("units")
    )
    .withColumn(
        "risk_severity",
        F.when(F.col("risk_occurrence_count") >= 10, "High")
        .when(F.col("risk_occurrence_count") >= 5, "Medium")
        .otherwise("Low")
    )
)

# Write to Gold
clinical_risk_indicators.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.clinical_risk_indicators")

print("✅ CLINICAL TABLE 4: clinical_risk_indicators")
print(f"Records: {clinical_risk_indicators.count():,}")

# COMMAND ----------

# DBTITLE 1,Cell 7: Optimize All Gold Tables
# MAGIC %sql
# MAGIC -- Cell 7: Optimize All Gold Tables
# MAGIC -- Apply Delta Lake optimizations for query performance
# MAGIC
# MAGIC -- Optimize demographics summary
# MAGIC OPTIMIZE healthcare.gold.patient_demographics_summary;
# MAGIC
# MAGIC -- Optimize financial metrics
# MAGIC OPTIMIZE healthcare.gold.healthcare_financial_metrics;
# MAGIC
# MAGIC -- Optimize lifetime analytics
# MAGIC OPTIMIZE healthcare.gold.patient_lifetime_analytics;
# MAGIC
# MAGIC -- Optimize quality metrics
# MAGIC OPTIMIZE healthcare.gold.data_quality_metrics;
# MAGIC
# MAGIC -- Clinical tables
# MAGIC OPTIMIZE healthcare.gold.patient_clinical_summary;
# MAGIC OPTIMIZE healthcare.gold.vital_signs_analytics;
# MAGIC OPTIMIZE healthcare.gold.observation_type_distribution;
# MAGIC OPTIMIZE healthcare.gold.clinical_risk_indicators;

# COMMAND ----------

# DBTITLE 1,Cell 8: Set Table Properties for Auto-Optimization
# MAGIC %sql
# MAGIC -- Cell 8: Set Table Properties for Auto-Optimization
# MAGIC -- Enable auto-optimize for future writes
# MAGIC
# MAGIC -- Demographics summary
# MAGIC ALTER TABLE healthcare.gold.patient_demographics_summary
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Patient counts by demographic attributes'
# MAGIC );
# MAGIC
# MAGIC -- Financial metrics
# MAGIC ALTER TABLE healthcare.gold.healthcare_financial_metrics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Healthcare financial KPIs and metrics'
# MAGIC );
# MAGIC
# MAGIC -- Lifetime analytics
# MAGIC ALTER TABLE healthcare.gold.patient_lifetime_analytics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Patient age and lifespan statistics'
# MAGIC );
# MAGIC
# MAGIC -- Quality metrics
# MAGIC ALTER TABLE healthcare.gold.data_quality_metrics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Data quality and completeness metrics'
# MAGIC );
# MAGIC
# MAGIC -- Clinical tables
# MAGIC ALTER TABLE healthcare.gold.patient_clinical_summary
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Patient clinical profiles with observation activity'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE healthcare.gold.vital_signs_analytics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Vital signs aggregated by demographics'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE healthcare.gold.observation_type_distribution
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Clinical measurement coverage and frequency'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE healthcare.gold.clinical_risk_indicators
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'description' = 'Gold: Patients with abnormal vital signs for risk stratification'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Cell 9: Verify Gold Tables - List All Tables
# MAGIC %sql
# MAGIC -- Cell 9: Verify Gold Tables - List All Tables
# MAGIC -- Show all tables in the gold schema
# MAGIC
# MAGIC SHOW TABLES IN healthcare.gold;

# COMMAND ----------

# DBTITLE 1,Cell 10: Verify Demographics Summary
# MAGIC %sql
# MAGIC -- Cell 10: Verify Demographics Summary
# MAGIC -- Check patient counts by gender and state
# MAGIC
# MAGIC SELECT 
# MAGIC   GENDER,
# MAGIC   STATE,
# MAGIC   SUM(patient_count) as total_patients,
# MAGIC   ROUND(AVG(avg_income), 0) as avg_income,
# MAGIC   SUM(deceased_count) as deceased
# MAGIC FROM healthcare.gold.patient_demographics_summary
# MAGIC GROUP BY GENDER, STATE
# MAGIC ORDER BY total_patients DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Cell 11: Verify Financial Metrics
# MAGIC %sql
# MAGIC -- Cell 11: Verify Financial Metrics
# MAGIC -- Business KPIs: expense-to-income ratio and coverage gap
# MAGIC
# MAGIC SELECT 
# MAGIC   GENDER,
# MAGIC   STATE,
# MAGIC   patient_count,
# MAGIC   ROUND(avg_income, 0) as avg_income,
# MAGIC   ROUND(avg_healthcare_expenses, 0) as avg_expenses,
# MAGIC   expense_to_income_ratio,
# MAGIC   ROUND(coverage_gap, 0) as coverage_gap
# MAGIC FROM healthcare.gold.healthcare_financial_metrics
# MAGIC ORDER BY patient_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Cell 12: Verify Lifetime Analytics
# MAGIC %sql
# MAGIC -- Cell 12: Verify Lifetime Analytics
# MAGIC -- Age and mortality statistics by demographics
# MAGIC
# MAGIC SELECT 
# MAGIC   GENDER,
# MAGIC   RACE,
# MAGIC   patient_status,
# MAGIC   patient_count,
# MAGIC   ROUND(avg_current_age, 1) as avg_age,
# MAGIC   ROUND(avg_age_at_death, 1) as avg_age_at_death
# MAGIC FROM healthcare.gold.patient_lifetime_analytics
# MAGIC ORDER BY patient_count DESC;

# COMMAND ----------

# DBTITLE 1,Cell 13: Verify Data Quality Metrics
# MAGIC %sql
# MAGIC -- Cell 13: Verify Data Quality Metrics
# MAGIC -- Overall data quality dashboard
# MAGIC
# MAGIC SELECT 
# MAGIC   total_records,
# MAGIC   unique_patients,
# MAGIC   quality_issues,
# MAGIC   clean_records,
# MAGIC   overall_completeness_pct,
# MAGIC   missing_birthdate,
# MAGIC   missing_gender,
# MAGIC   missing_income,
# MAGIC   measurement_timestamp
# MAGIC FROM healthcare.gold.data_quality_metrics;

# COMMAND ----------

# DBTITLE 1,Summary - Professional Patterns Demonstrated
# MAGIC %md
# MAGIC # 🎯 Summary: Professional Gold Layer Patterns
# MAGIC
# MAGIC ## What We Built
# MAGIC
# MAGIC ### ✅ 4 Gold Tables Created:
# MAGIC
# MAGIC 1. **patient_demographics_summary** (Dimensional)
# MAGIC    - Patient counts by demographics and geography
# MAGIC    - Foundation for population health dashboards
# MAGIC
# MAGIC 2. **healthcare_financial_metrics** (Fact/KPI)
# MAGIC    - Financial metrics: income, expenses, coverage
# MAGIC    - Business-calculated fields: expense_to_income_ratio, coverage_gap
# MAGIC    - Ready for executive dashboards
# MAGIC
# MAGIC 3. **patient_lifetime_analytics** (Analytical)
# MAGIC    - Age and mortality statistics
# MAGIC    - Supports actuarial and trend analysis
# MAGIC
# MAGIC 4. **data_quality_metrics** (Operational)
# MAGIC    - Completeness and quality tracking
# MAGIC    - SLA monitoring and governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Professional Exam Topics Covered
# MAGIC
# MAGIC ### 1. **Dimensional Modeling**
# MAGIC - Facts vs Dimensions distinction
# MAGIC - Appropriate grain selection (demographic segments)
# MAGIC - Normalized aggregates for BI tools
# MAGIC
# MAGIC ### 2. **Business Metrics Design**
# MAGIC - Derived/calculated fields (expense_to_income_ratio)
# MAGIC - KPIs aligned with business questions
# MAGIC - Pre-aggregated for dashboard performance
# MAGIC
# MAGIC ### 3. **Multi-Hop Architecture**
# MAGIC - **Bronze** (raw) → **Silver** (cleaned) → **Gold** (business-ready)
# MAGIC - Each layer serves different consumers
# MAGIC - Proper separation of concerns
# MAGIC
# MAGIC ### 4. **Performance Optimization**
# MAGIC - Pre-aggregation reduces query complexity
# MAGIC - OPTIMIZE for file compaction
# MAGIC - Auto-optimize enabled for future writes
# MAGIC
# MAGIC ### 5. **Production Patterns**
# MAGIC - Consistent naming conventions
# MAGIC - Processing timestamps for lineage
# MAGIC - Table properties and descriptions for governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps for Professional Exam Prep
# MAGIC
# MAGIC ### Advanced Topics to Add:
# MAGIC
# MAGIC 1. **Incremental Refresh Pattern**
# MAGIC    - Change Data Feed (CDF) from Silver
# MAGIC    - Incremental aggregation logic
# MAGIC    - Merge strategy for Gold updates
# MAGIC
# MAGIC 2. **Partitioning Strategy**
# MAGIC    - Partition Gold tables by `processing_date`
# MAGIC    - Z-ORDER or LIQUID CLUSTERING on high-cardinality columns
# MAGIC    - Partition pruning for time-range queries
# MAGIC
# MAGIC 3. **Pipeline Orchestration**
# MAGIC    - Multi-task job: Bronze → Silver → Gold
# MAGIC    - Task dependencies and error handling
# MAGIC    - Job-level retry and alerting
# MAGIC
# MAGIC 4. **Monitoring & Alerting**
# MAGIC    - Track Gold table freshness
# MAGIC    - Alert on data quality threshold breaches
# MAGIC    - Pipeline health metrics
# MAGIC
# MAGIC 5. **Cost Optimization**
# MAGIC    - Analyze read patterns
# MAGIC    - Optimize file sizes (small file compaction)
# MAGIC    - Review partition strategy for efficiency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Professional Exam Question Examples
# MAGIC
# MAGIC **Scenario 1:** "Your Gold financial_metrics table is slow for queries filtering by STATE. What optimization would you apply?"
# MAGIC - **Answer:** Z-ORDER BY (STATE) or use LIQUID CLUSTERING on STATE
# MAGIC
# MAGIC **Scenario 2:** "Business wants Gold tables refreshed hourly, but Silver gets 1M updates/hour. How do you optimize Gold refresh?"
# MAGIC - **Answer:** Use CDF from Silver for incremental refresh; MERGE into Gold instead of full overwrite
# MAGIC
# MAGIC **Scenario 3:** "Data quality_metrics shows 15% quality_issues. How would you investigate and remediate?"
# MAGIC - **Answer:** Query Silver for data_quality_flag=1 records; trace back to Bronze source_file; fix ingestion schema or add validation rules
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Your Patient360 Pipeline is Production-Ready!
# MAGIC
# MAGIC **Bronze → Silver → Gold** architecture complete with Professional-level patterns.

# COMMAND ----------

