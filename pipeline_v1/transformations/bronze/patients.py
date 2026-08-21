# Bronze Layer - Patient Demographics
# Ingests patient data from CSV files using Auto Loader
# Pattern: Incremental file ingestion with schema evolution support

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="healthcare.bronze.pipeline_patients_bronze",
    comment="Raw patient demographics ingested from CSV files with Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
# Professional Exam Pattern: Three-tier expectation strategy at ingestion
# expect_or_fail: Business-critical keys (pipeline stops on violation)
@dp.expect_or_fail("valid_patient_id", "Id IS NOT NULL")
# expect_or_drop: Data quality gates (quarantine bad records)
@dp.expect_or_drop("valid_birthdate", "BIRTHDATE IS NOT NULL")
@dp.expect_or_drop("valid_gender", "GENDER IN ('M', 'F')")
# expect: Monitoring/alerting (track anomalies without dropping)
@dp.expect("birthdate_in_past", "BIRTHDATE <= CURRENT_DATE()")
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
def pipeline_patients_bronze():
    """
    Bronze streaming table for patient demographics.
    
    Features:
    - Auto Loader for incremental file processing
    - Automatic schema inference with type detection
    - _rescued_data column for schema evolution
    - Metadata columns for lineage tracking
    - Handles multiple patient snapshot files (patients*.csv)
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")  # Infer proper types, not all STRING
        .option("header", "true")
        .load("/Volumes/healthcare/bronze/raw_files/patients*.csv")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )