# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Layer Transformation - Exam Prep Overview
# MAGIC %md
# MAGIC # Silver Layer: Patient Data Transformation
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC ### Transformation Pipeline:
# MAGIC
# MAGIC **1. Type Casting**
# MAGIC - STRING → DATE conversions
# MAGIC - STRING → INT/DOUBLE conversions
# MAGIC - Handling cast failures and nulls
# MAGIC
# MAGIC **2. Data Quality Management**
# MAGIC - Handling malformed records (`_rescued_data`)
# MAGIC - Null value handling strategies
# MAGIC - Adding quality flags/metrics
# MAGIC
# MAGIC **3. Deduplication Strategies**
# MAGIC - Window functions for duplicate identification
# MAGIC - Time-based record selection (most recent)
# MAGIC - Row number partitioning
# MAGIC
# MAGIC **4. Delta Lake Operations**
# MAGIC - Writing to managed Delta tables
# MAGIC - Overwrite vs Append modes
# MAGIC - Table properties and optimization
# MAGIC
# MAGIC **5. Processing Metadata**
# MAGIC - Adding audit columns
# MAGIC - Timestamp tracking
# MAGIC - Data lineage markers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Source:** `healthcare.bronze.patient_raw_auto` (113 rows)  
# MAGIC **Target:** `healthcare.silver.patients`

# COMMAND ----------

# DBTITLE 1,Cell 1: Read Bronze Layer Data
# Cell 1: Read Bronze Layer Data (OPTIMIZED)
# Read from Delta table and inspect structure

from pyspark.sql import functions as F
from pyspark.sql import Window

# Read the Bronze layer table
bronze_df = spark.table("healthcare.bronze.patient_raw_auto")

print(f"✅ Bronze table loaded")
print(f"\n📊 Schema:")
bronze_df.printSchema()

# Display sample (counts rows as side effect)
print(f"\n🔍 Sample data:")
display(bronze_df.limit(5))

# Get row count from display action
row_count = bronze_df.count()
print(f"\nTotal rows: {row_count}")

# COMMAND ----------

# DBTITLE 1,Cell 2: Inspect Data Quality Issues
# Cell 2: Inspect Data Quality Issues (OPTIMIZED - Single Pass)
# Identify nulls, rescued data, and data quality problems in ONE aggregation

# Single-pass aggregation for all quality checks
quality_metrics = bronze_df.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_count"),
    F.sum(F.when(F.col("BIRTHDATE").isNull(), 1).otherwise(0)).alias("birthdate_nulls"),
    F.sum(F.when(F.col("DEATHDATE").isNull(), 1).otherwise(0)).alias("deathdate_nulls"),
    F.sum(F.when(F.col("SSN").isNull(), 1).otherwise(0)).alias("ssn_nulls"),
    F.sum(F.when(F.col("GENDER").isNull(), 1).otherwise(0)).alias("gender_nulls"),
    F.sum(F.when(F.col("INCOME").isNull(), 1).otherwise(0)).alias("income_nulls")
).collect()[0]

total = quality_metrics["total_rows"]
print(f"⚠️  Malformed records (_rescued_data): {quality_metrics['rescued_count']}")

print(f"\n📈 Null Analysis:")
for col_name in ['BIRTHDATE', 'DEATHDATE', 'SSN', 'GENDER', 'INCOME']:
    null_count = quality_metrics[f"{col_name.lower()}_nulls"]
    null_pct = (null_count / total * 100) if total > 0 else 0
    print(f"  {col_name}: {null_count} nulls ({null_pct:.1f}%)")

# Check for duplicate IDs (separate query)
dup_ids = bronze_df.groupBy("Id").count().filter(F.col("count") > 1)
print(f"\n🔄 Duplicate IDs: {dup_ids.count()}")

# COMMAND ----------

# DBTITLE 1,Cell 3: Type Casting - Dates (KEY EXAM TOPIC)
# Cell 3: Type Casting - Convert STRING to DATE
# Use to_date() for date conversions

# Cast BIRTHDATE and DEATHDATE from STRING to DATE
# Note: to_date() returns NULL for invalid dates (graceful failure)
silver_df = bronze_df.withColumn(
    "BIRTHDATE", 
    F.to_date(F.col("BIRTHDATE"), "yyyy-MM-dd")  # Explicit date format
).withColumn(
    "DEATHDATE", 
    F.to_date(F.col("DEATHDATE"), "yyyy-MM-dd")  # Empty strings become NULL
)

print("✅ Date conversions completed")
# Note: Validation deferred to final verification cells to avoid eager execution

# COMMAND ----------

# DBTITLE 1,Cell 4: Type Casting - Integers (KEY EXAM TOPIC)
# Cell 4: Type Casting - Convert STRING to INT
# Use cast() for integer conversions

# Cast FIPS, ZIP, and INCOME from STRING to INT
# Note: FIPS contains decimal strings (e.g., '25025.0'), use TRY_CAST for safety
# TRY_CAST returns NULL for malformed values instead of raising an error

silver_df = silver_df \
    .withColumn("FIPS", F.expr("TRY_CAST(FIPS AS INT)")) \
    .withColumn("ZIP", F.expr("TRY_CAST(ZIP AS INT)")) \
    .withColumn("INCOME", F.expr("TRY_CAST(INCOME AS INT)"))

print("✅ Integer conversions completed")
# Note: Validation deferred to final verification cells to avoid eager execution

# COMMAND ----------

# DBTITLE 1,Cell 5: Type Casting - Decimals (KEY EXAM TOPIC)
# Cell 5: Type Casting - Convert STRING to DOUBLE
# Use cast() for decimal conversions

# Cast LAT, LON, HEALTHCARE_EXPENSES, HEALTHCARE_COVERAGE to DOUBLE
silver_df = silver_df \
    .withColumn("LAT", F.col("LAT").cast("double")) \
    .withColumn("LON", F.col("LON").cast("double")) \
    .withColumn("HEALTHCARE_EXPENSES", F.col("HEALTHCARE_EXPENSES").cast("double")) \
    .withColumn("HEALTHCARE_COVERAGE", F.col("HEALTHCARE_COVERAGE").cast("double"))

print("✅ Double conversions completed")
# Note: Validation deferred to final verification cells to avoid eager execution

# COMMAND ----------

# DBTITLE 1,Cell 6: Deduplication with Window Functions (KEY EXAM TOPIC)
# Cell 6: Deduplication - Keep Most Recent Record
# Use Window functions with row_number() for deduplication

# Define window partitioned by Id, ordered by ingestion_timestamp DESC
# This assigns row_number=1 to the most recent record for each Id
window_spec = Window.partitionBy("Id").orderBy(F.col("ingestion_timestamp").desc())

# Add row number and filter to keep only the most recent
silver_df_dedup = silver_df \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")  # Remove helper column

print(f"✅ Deduplication logic applied")

# Replace silver_df with deduplicated version
silver_df = silver_df_dedup

# Note: Dedup metrics deferred to avoid triggering actions mid-pipeline

# COMMAND ----------

# DBTITLE 1,Cell 7: Add Data Quality Flags and Processing Metadata
# Cell 7: Add Data Quality Flags and Processing Metadata
# Add audit columns, quality metrics, and data lineage

# Add processing timestamp (when transformation occurred)
silver_df = silver_df.withColumn(
    "processing_timestamp", 
    F.current_timestamp()
)

# Add processing date (partition key candidate)
silver_df = silver_df.withColumn(
    "processing_date", 
    F.current_date()
)

# Add data quality flag (0 = clean, 1 = has issues)
# Flag records with missing critical data or rescued data
silver_df = silver_df.withColumn(
    "data_quality_flag",
    F.when(
        (F.col("_rescued_data").isNotNull()) |  # Malformed records
        (F.col("BIRTHDATE").isNull()) |          # Missing birth date
        (F.col("GENDER").isNull()),              # Missing gender
        1
    ).otherwise(0)
)

print(f"✅ Quality flags and processing metadata added")
# Note: Validation deferred to final verification cells

# COMMAND ----------

# DBTITLE 1,Cell 8: Select Final Columns for Silver Layer
# Cell 8: Select Final Columns for Silver Layer
# Select and order columns for final Silver schema

# Select and order columns for Silver table
# Drop Bronze audit columns, keep Silver processing metadata
silver_final = silver_df.select(
    # Business keys
    "Id",
    "SSN",
    
    # Demographics (with proper types)
    "BIRTHDATE",        # DATE
    "DEATHDATE",        # DATE
    "GENDER",
    "RACE",
    "ETHNICITY",
    "MARITAL",
    
    # Names
    "PREFIX",
    "FIRST",
    "MIDDLE",
    "LAST",
    "SUFFIX",
    "MAIDEN",
    
    # Location (with proper types)
    "ADDRESS",
    "CITY",
    "STATE",
    "COUNTY",
    "BIRTHPLACE",
    "ZIP",              # INT
    "FIPS",             # INT
    "LAT",              # DOUBLE
    "LON",              # DOUBLE
    
    # Financial (with proper types)
    "INCOME",                    # INT
    "HEALTHCARE_EXPENSES",       # DOUBLE
    "HEALTHCARE_COVERAGE",       # DOUBLE
    
    # IDs
    "DRIVERS",
    "PASSPORT",
    
    # Silver metadata
    "data_quality_flag",
    "processing_timestamp",
    "processing_date",
    
    # Bronze lineage (optional - for traceability)
    "source_file",
    "ingestion_timestamp"
)

print(f"✅ Final Silver schema prepared")
print(f"\n📋 Schema:")
silver_final.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 9: Write to Silver Delta Table
# Cell 9: Write to Silver Delta Table (PROFESSIONAL PATTERN)
# Defensive write pattern with validation and control flag

# ==================== CONFIGURATION ====================
WRITE_ENABLED = True  # Set to False to preview without writing
target_table = "healthcare.silver.patients"
# =======================================================

# Pre-write validation: Count rows to be written
rows_to_write = silver_final.count()
print(f"📊 Rows prepared for write: {rows_to_write}")

# Check if target table exists
table_exists = spark.catalog.tableExists(target_table)

if table_exists:
    print(f"\n⚠️  Table '{target_table}' already exists")
    
    # Show current table stats for comparison
    current_count = spark.table(target_table).count()
    print(f"   Current rows: {current_count}")
    print(f"   New rows: {rows_to_write}")
    print(f"   Delta: {rows_to_write - current_count:+d}")
    
    # Show schema comparison
    current_schema = spark.table(target_table).schema
    new_schema = silver_final.schema
    
    if current_schema != new_schema:
        print(f"\n⚠️  Schema change detected")
        print(f"   Current columns: {len(current_schema)}")
        print(f"   New columns: {len(new_schema)}")
else:
    print(f"\n✨ Table '{target_table}' does not exist (will be created)")

# Write operation (controlled by flag)
if WRITE_ENABLED:
    print(f"\n🚀 Writing to '{target_table}'...")
    
    silver_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    
    print("✅ Silver table written successfully")
    print(f"   Table: {target_table}")
    print(f"   Mode: overwrite")
    print(f"   Rows written: {rows_to_write}")
else:
    print(f"\n⏸️  Write SKIPPED (WRITE_ENABLED = False)")
    print(f"   Set WRITE_ENABLED = True to execute write operation")
    print(f"   This is a safety feature to prevent accidental overwrites")

# COMMAND ----------

# DBTITLE 1,Cell 10: Optimize Delta Table
# Cell 10: Optimize Delta Table and Add Table Properties
# Compact small files and configure table properties

target_table = "healthcare.silver.patients"

# Check if table exists before optimization
if spark.catalog.tableExists(target_table):
    # Optimize the table (compact small files)
    spark.sql(f"OPTIMIZE {target_table}")
    
    print("✅ Table optimized (small files compacted)")
    
    # Add table properties and comments (governance)
    spark.sql(f"""
        ALTER TABLE {target_table}
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'description' = 'Silver layer: Cleaned and typed patient data'
        )
    """)
    
    print("✅ Table properties configured")
    print("  - Auto optimize write: enabled")
    print("  - Auto compact: enabled")
else:
    print(f"⚠️  Table '{target_table}' does not exist")
    print("   Run Cell 9 with WRITE_ENABLED = True first to create the table")

# COMMAND ----------

# DBTITLE 1,Cell 11: Verify Silver Table - Row Counts and Schema
# MAGIC %sql
# MAGIC -- Cell 11: Verify Silver Table - Row Counts and Schema
# MAGIC -- Validate row counts and check for quality issues
# MAGIC
# MAGIC -- Check row count
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_patients,
# MAGIC   COUNT(DISTINCT Id) as unique_ids,
# MAGIC   SUM(CASE WHEN data_quality_flag = 1 THEN 1 ELSE 0 END) as quality_issues
# MAGIC FROM healthcare.silver.patients;

# COMMAND ----------

# DBTITLE 1,Cell 12: Verify Type Conversions
# MAGIC %sql
# MAGIC -- Cell 12: Verify Type Conversions
# MAGIC -- Check data types after transformation
# MAGIC
# MAGIC DESCRIBE healthcare.silver.patients;

# COMMAND ----------

# DBTITLE 1,Cell 13: Sample Transformed Data
# MAGIC %sql
# MAGIC -- Cell 13: Sample Transformed Data
# MAGIC -- Verify the final output
# MAGIC
# MAGIC SELECT 
# MAGIC   Id,
# MAGIC   FIRST,
# MAGIC   LAST,
# MAGIC   BIRTHDATE,           -- Now DATE type
# MAGIC   YEAR(BIRTHDATE) as birth_year,
# MAGIC   INCOME,              -- Now INT type
# MAGIC   HEALTHCARE_EXPENSES, -- Now DOUBLE type
# MAGIC   LAT,                 -- Now DOUBLE type
# MAGIC   LON,                 -- Now DOUBLE type
# MAGIC   data_quality_flag,
# MAGIC   processing_timestamp
# MAGIC FROM healthcare.silver.patients
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Cell 14: Data Quality Summary
# MAGIC %sql
# MAGIC -- Cell 14: Data Quality Summary
# MAGIC -- Summary of quality metrics and key statistics
# MAGIC
# MAGIC SELECT
# MAGIC   'Total Patients' as metric,
# MAGIC   COUNT(*) as value
# MAGIC FROM healthcare.silver.patients
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Deceased Patients',
# MAGIC   COUNT(*)
# MAGIC FROM healthcare.silver.patients
# MAGIC WHERE DEATHDATE IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Quality Issues',
# MAGIC   COUNT(*)
# MAGIC FROM healthcare.silver.patients
# MAGIC WHERE data_quality_flag = 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Avg Healthcare Expenses',
# MAGIC   CAST(AVG(HEALTHCARE_EXPENSES) AS BIGINT)
# MAGIC FROM healthcare.silver.patients
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Avg Income',
# MAGIC   CAST(AVG(INCOME) AS BIGINT)
# MAGIC FROM healthcare.silver.patients;

# COMMAND ----------

# DBTITLE 1,Summary: Transformation Concepts
# MAGIC %md
# MAGIC # 🎯 Summary: Transformation Concepts
# MAGIC
# MAGIC ## Data Transformation and Modeling
# MAGIC
# MAGIC ### ✅ Concepts Demonstrated:
# MAGIC
# MAGIC **1. Type Casting (Cells 3-5)**
# MAGIC - STRING → DATE using `to_date()`
# MAGIC - STRING → INT using `cast()`
# MAGIC - STRING → DOUBLE using `cast()`
# MAGIC - Handling cast failures (NULLs)
# MAGIC
# MAGIC **2. Data Quality (Cells 2, 7)**
# MAGIC - Identifying malformed records (`_rescued_data`)
# MAGIC - Null analysis and handling
# MAGIC - Quality flag creation
# MAGIC - Data validation checks
# MAGIC
# MAGIC **3. Deduplication (Cell 6)**
# MAGIC - Window functions (`Window.partitionBy()`)
# MAGIC - `row_number()` for ranking
# MAGIC - Keeping most recent records
# MAGIC
# MAGIC **4. Delta Lake Operations (Cells 9-10)**
# MAGIC - Writing to managed Delta tables
# MAGIC - `overwrite` vs `append` modes
# MAGIC - `OPTIMIZE` for file compaction
# MAGIC - Table properties (auto-optimize)
# MAGIC
# MAGIC **5. Processing Metadata (Cell 7)**
# MAGIC - Audit columns (`processing_timestamp`, `processing_date`)
# MAGIC - Data lineage tracking
# MAGIC - Quality metrics
# MAGIC
# MAGIC **6. SQL Verification (Cells 11-14)**
# MAGIC - Row count validation
# MAGIC - Schema inspection (`DESCRIBE`)
# MAGIC - Quality metrics aggregation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Run all cells** to create the Silver table
# MAGIC 2. **Verify transformation results** (Cells 11-14)
# MAGIC 3. **Chain Bronze → Silver** in a multi-task job