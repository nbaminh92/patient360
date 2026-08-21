# Databricks notebook source
# DBTITLE 1,Silver Layer: Observation Transformations
# MAGIC %md
# MAGIC # Silver Layer: Observation Transformations
# MAGIC
# MAGIC ## Purpose
# MAGIC Transform raw Bronze observation data into cleaned, typed Silver layer with:
# MAGIC * Type-safe casting (try_cast for error resilience)
# MAGIC * Deduplication logic
# MAGIC * Quality flags for governance
# MAGIC * Schema validation
# MAGIC * Defensive write patterns
# MAGIC
# MAGIC ## Professional Exam Topics
# MAGIC * Multi-entity Silver processing
# MAGIC * Consistent quality frameworks across entities
# MAGIC * Type safety and error handling
# MAGIC * Production-grade defensive patterns
# MAGIC * Parallel layer transformations

# COMMAND ----------

# DBTITLE 1,Configuration: Defensive Write Pattern
# Defensive pattern - prevent accidental overwrites
WRITE_ENABLED = True  # Set to True when ready to write

# Table references
source_table = "healthcare.bronze.observation_raw"
target_table = "healthcare.silver.observations"

print(f"Source: {source_table}")
print(f"Target: {target_table}")
print(f"Write Mode: {'ENABLED ✅' if WRITE_ENABLED else 'PREVIEW ONLY ⚠️'}")

# COMMAND ----------

# DBTITLE 1,Step 1: Read Bronze and Apply Type-Safe Casting
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Bronze table
df_bronze = spark.table(source_table)

print(f"Bronze records: {df_bronze.count()}")
print("\nBronze schema:")
df_bronze.printSchema()

# Type-safe transformations with try_cast
df_typed = df_bronze.select(
    # Identifiers (strings, should already be clean)
    F.col("DATE").alias("observation_date"),
    F.col("PATIENT").alias("patient_id"),
    F.col("ENCOUNTER").alias("encounter_id"),
    F.col("CODE").alias("observation_code"),
    F.col("DESCRIPTION").alias("observation_description"),
    
    # Numeric fields with try_cast for resilience
    F.col("VALUE").try_cast("double").alias("observation_value"),
    
    # Text fields
    F.col("UNITS").alias("units"),
    F.col("TYPE").alias("observation_type"),
    F.col("CATEGORY").alias("category")
)

print(f"\nTyped records: {df_typed.count()}")

# COMMAND ----------

# DBTITLE 1,Step 2: Add Quality Flags
# Add quality flags for governance
df_quality = df_typed.withColumns({
    "missing_date": F.when(F.col("observation_date").isNull(), 1).otherwise(0),
    "missing_patient": F.when(F.col("patient_id").isNull(), 1).otherwise(0),
    "missing_value": F.when(F.col("observation_value").isNull(), 1).otherwise(0),
    "missing_code": F.when(F.col("observation_code").isNull(), 1).otherwise(0)
})

# Overall quality flag
df_quality = df_quality.withColumn(
    "has_quality_issue",
    F.when(
        (F.col("missing_date") == 1) |
        (F.col("missing_patient") == 1) |
        (F.col("missing_value") == 1) |
        (F.col("missing_code") == 1),
        1
    ).otherwise(0)
)

# Quality summary
quality_issues = df_quality.filter(F.col("has_quality_issue") == 1).count()
print(f"\nQuality Issues: {quality_issues} records")
print(f"Clean Records: {df_quality.filter(F.col('has_quality_issue') == 0).count()}")

# COMMAND ----------

# DBTITLE 1,Step 3: Deduplication Logic
# Deduplication - keep most recent record per observation
# Assuming combination of (patient_id, observation_date, observation_code) is unique

window_spec = Window.partitionBy(
    "patient_id", 
    "observation_date", 
    "observation_code"
).orderBy(F.desc("observation_date"))

df_dedup = df_quality.withColumn(
    "row_num",
    F.row_number().over(window_spec)
).filter(
    F.col("row_num") == 1
).drop("row_num")

total_records = df_quality.count()
duplicate_records = total_records - df_dedup.count()

print(f"\nTotal records: {total_records}")
print(f"Duplicates removed: {duplicate_records}")
print(f"Final unique records: {df_dedup.count()}")

# COMMAND ----------

# DBTITLE 1,Step 4: Schema Validation
# Validate expected schema
expected_columns = [
    "observation_date", "patient_id", "encounter_id", "observation_code",
    "observation_description", "observation_value", "units", "observation_type",
    "category", "missing_date", "missing_patient", "missing_value",
    "missing_code", "has_quality_issue"
]

actual_columns = df_dedup.columns
missing_columns = set(expected_columns) - set(actual_columns)
extra_columns = set(actual_columns) - set(expected_columns)

print("\n=== SCHEMA VALIDATION ===")
print(f"Expected columns: {len(expected_columns)}")
print(f"Actual columns: {len(actual_columns)}")

if missing_columns:
    print(f"⚠️  Missing columns: {missing_columns}")
if extra_columns:
    print(f"⚠️  Extra columns: {extra_columns}")
if not missing_columns and not extra_columns:
    print("✅ Schema validation passed")

print("\nFinal Silver schema:")
df_dedup.printSchema()

# COMMAND ----------

# DBTITLE 1,Step 5: Preview Before Write
# Preview data before write
print("\n=== PREVIEW: First 5 rows ===")
df_dedup.show(5, truncate=False)

print("\n=== SUMMARY STATISTICS ===")
df_dedup.select(
    F.count("*").alias("total_observations"),
    F.countDistinct("patient_id").alias("unique_patients"),
    F.countDistinct("observation_code").alias("unique_observation_types"),
    F.sum("has_quality_issue").alias("quality_issues"),
    F.avg("observation_value").alias("avg_observation_value")
).show()

print("\n=== OBSERVATION TYPE BREAKDOWN ===")
df_dedup.groupBy("observation_type").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# DBTITLE 1,Step 6: Defensive Write to Silver
if WRITE_ENABLED:
    print("\n=== WRITING TO SILVER TABLE ===")
    
    # Write with CREATE OR REPLACE (overwrites)
    df_dedup.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"✅ Successfully wrote {df_dedup.count()} records to {target_table}")
    
    # Verify write
    written_count = spark.table(target_table).count()
    print(f"✅ Verified: {written_count} records in Silver table")
    
    # Optimize table if it exists
    print("\n=== OPTIMIZING TABLE ===")
    spark.sql(f"OPTIMIZE {target_table}")
    print(f"✅ Table optimized")
    
else:
    print("\n⚠️  WRITE_ENABLED is False - Preview only")
    print("Set WRITE_ENABLED = True to write to Silver table")

# COMMAND ----------

# DBTITLE 1,Validation: Query Silver Table
# MAGIC %sql
# MAGIC -- Query Silver observations table
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_observations,
# MAGIC   COUNT(DISTINCT patient_id) as unique_patients,
# MAGIC   COUNT(DISTINCT observation_code) as unique_observation_types,
# MAGIC   SUM(has_quality_issue) as quality_issues,
# MAGIC   ROUND(AVG(observation_value), 2) as avg_value
# MAGIC FROM healthcare.silver.observations;
# MAGIC
# MAGIC -- Sample observations
# MAGIC SELECT * FROM healthcare.silver.observations LIMIT 10;

# COMMAND ----------

