# Bronze Layer - Clinical Observations
# Ingests observation data (vital signs, lab results) from CSV using Auto Loader
# Pattern: High-volume streaming ingestion with schema evolution

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="healthcare.bronze.pipeline_observations_bronze",
    comment="Raw clinical observations (vital signs, lab results) ingested with Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
# Professional Exam Pattern: Three-tier expectation strategy at ingestion
# expect_or_fail: Critical referential integrity (orphaned observations are unusable)
@dp.expect_or_fail("valid_patient_ref", "PATIENT IS NOT NULL")
# expect_or_drop: Data quality gates (drop incomplete clinical records)
@dp.expect_or_drop("valid_observation_date", "DATE IS NOT NULL")
@dp.expect_or_drop("valid_observation_code", "CODE IS NOT NULL")
# expect: Monitoring/alerting (track anomalies without dropping)
@dp.expect("has_observation_value", "VALUE IS NOT NULL")
@dp.expect("observation_in_past", "DATE <= CURRENT_TIMESTAMP()")
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
def pipeline_observations_bronze():
    """
    Bronze streaming table for clinical observations.
    
    Features:
    - Auto Loader for incremental processing of 20MB+ observation files
    - Schema inference with type detection for numeric values
    - _rescued_data captures schema drift (new LOINC codes, units, etc.)
    - Metadata columns track source file and ingestion time
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/Volumes/healthcare/bronze/raw_files/observations*.csv")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )