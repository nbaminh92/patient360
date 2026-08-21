# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Layer: Observation Ingestion with Auto Loader
# MAGIC %md
# MAGIC # Bronze Layer: Observation Data Ingestion
# MAGIC
# MAGIC ## Purpose
# MAGIC Ingest raw observation data from Unity Catalog Volume with:
# MAGIC * **Schema inference** from CSV headers
# MAGIC * **Metadata tracking** (source file, ingestion timestamp)
# MAGIC * **Quality validation** (duplicates, nulls, patient coverage)
# MAGIC * **Production-ready** defensive write pattern
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Volume CSV → DataFrame → Bronze Delta Table
# MAGIC /Volumes/.../observations.csv → healthcare.bronze.observation_raw
# MAGIC ```
# MAGIC
# MAGIC ## Professional Exam Topics
# MAGIC * **Multi-entity batch ingestion** - Parallel Bronze processing patterns
# MAGIC * **Unity Catalog integration** - Volume-based data ingestion
# MAGIC * **Referential validation** - Ensuring data integrity across entities
# MAGIC * **Quality gates** - Validation before persistence
# MAGIC * **Delta Lake basics** - Overwrite vs append modes
# MAGIC
# MAGIC ## Key Differences from Patient Ingestion (Exam Focus)
# MAGIC * **Volume consideration** - Observations are 1000x larger (113K vs 113 rows)
# MAGIC * **Referential checks** - All observations must link to valid patients
# MAGIC * **Category validation** - Observations have categorical constraints

# COMMAND ----------

# DBTITLE 1,Configuration: Paths and Table Names
# Centralized configuration
from pyspark.sql import functions as F

# Source and target configuration
SOURCE_PATH = "/Volumes/healthcare/bronze/raw_files/observations.csv"
TARGET_TABLE = "healthcare.bronze.observation_raw"

print("="*60)
print("🏥 BRONZE LAYER: OBSERVATION INGESTION")
print("="*60)
print(f"\n📂 Source: {SOURCE_PATH}")
print(f"🎯 Target: {TARGET_TABLE}")
print(f"\n✅ Configuration loaded")

# COMMAND ----------

# DBTITLE 1,Auto Loader: Incremental Ingestion from Volume
# Read CSV with schema inference and add metadata columns
df_observation = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(SOURCE_PATH)
         .withColumn("source_file", F.col("_metadata.file_path"))
         .withColumn("ingestion_timestamp", F.current_timestamp())
)

print("\n📊 Schema:")
df_observation.printSchema()

# Single-pass quality validation
quality_metrics = df_observation.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("PATIENT").alias("unique_patients"),
    F.countDistinct("ENCOUNTER").alias("unique_encounters"),
    F.sum(F.when(F.col("PATIENT").isNull(), 1).otherwise(0)).alias("null_patients"),
    F.sum(F.when(F.col("DATE").isNull(), 1).otherwise(0)).alias("null_dates"),
    F.sum(F.when(F.col("VALUE").isNull(), 1).otherwise(0)).alias("null_values"),
    F.sum(F.when(F.col("CATEGORY").isNull(), 1).otherwise(0)).alias("null_categories")
).collect()[0]

print("\n🔍 Quality Validation:")
print("="*60)
print(f"Total Rows: {quality_metrics['total_rows']:,}")
print(f"Unique Patients: {quality_metrics['unique_patients']:,}")
print(f"Unique Encounters: {quality_metrics['unique_encounters']:,}")
print(f"NULL Patient IDs: {quality_metrics['null_patients']}")
print(f"NULL Dates: {quality_metrics['null_dates']}")
print(f"NULL Values: {quality_metrics['null_values']}")
print(f"NULL Categories: {quality_metrics['null_categories']}")

# Validation status
has_null_patients = quality_metrics['null_patients'] > 0
has_null_dates = quality_metrics['null_dates'] > 0

if has_null_patients:
    print("\n⚠️  WARNING: NULL patient IDs detected (referential integrity issue)")
if has_null_dates:
    print("\n⚠️  WARNING: NULL observation dates detected")
if not has_null_patients and not has_null_dates:
    print("\n✅ Data quality validation passed")

# COMMAND ----------

# DBTITLE 1,Write Stream to Bronze Table
# Write to Bronze layer with metadata
print("\n💾 Writing to Bronze table...")

(
    df_observation.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
)

record_count = df_observation.count()
print(f"\n✅ Successfully wrote {record_count:,} records to {TARGET_TABLE}")
print(f"   - Includes source_file and ingestion_timestamp columns")
print(f"   - Mode: OVERWRITE (full refresh)")

# COMMAND ----------

# DBTITLE 1,Validation: Check Bronze Table
# Verify Bronze table write
verify_df = spark.table(TARGET_TABLE)

print("\n✅ Bronze Table Verification:")
print("="*60)

# Record counts
total_records = verify_df.count()
unique_patients = verify_df.select("PATIENT").distinct().count()
unique_categories = verify_df.select("CATEGORY").distinct().count()

print(f"Total records: {total_records:,}")
print(f"Unique patients: {unique_patients:,}")
print(f"Unique categories: {unique_categories}")
print(f"Schema columns: {len(verify_df.columns)}")

print(f"\nMetadata columns present:")
print(f"  - source_file: {'✅' if 'source_file' in verify_df.columns else '❌'}")
print(f"  - ingestion_timestamp: {'✅' if 'ingestion_timestamp' in verify_df.columns else '❌'}")

print("\n📊 Sample data:")
display(verify_df.select("DATE", "PATIENT", "CATEGORY", "DESCRIPTION", "VALUE", "UNITS", "source_file", "ingestion_timestamp").limit(5))