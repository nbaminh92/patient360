# Bronze Layer - Healthcare Organizations
# Ingests organization/facility data from CSV using Auto Loader
# Pattern: Reference data ingestion with schema evolution

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="healthcare.bronze.pipeline_organizations_bronze",
    comment="Raw healthcare organization/facility data ingested with Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
# Professional Exam Pattern: Three-tier expectation strategy at ingestion
# expect_or_fail: Critical dimension keys (pipeline stops on violation)
@dp.expect_or_fail("valid_org_id", "Id IS NOT NULL")
# expect_or_drop: Data quality gates (cleanse invalid reference data)
@dp.expect_or_drop("valid_org_name", "NAME IS NOT NULL AND LENGTH(TRIM(NAME)) > 0")
# expect: Monitoring/alerting (track anomalies without dropping)
@dp.expect("non_negative_revenue", "REVENUE IS NULL OR REVENUE >= 0")
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
def pipeline_organizations_bronze():
    """
    Bronze streaming table for healthcare organizations.
    
    Features:
    - Auto Loader for incremental processing of organization files
    - Schema inference with type detection
    - _rescued_data for schema evolution (new facility attributes)
    - Metadata columns for lineage
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/Volumes/healthcare/bronze/raw_files/organizations*.csv")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )