# Silver Layer - Healthcare Organizations
# Pattern: Simple cleansing for reference dimension data
# Professional Exam Focus: Reference data validation

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="healthcare.silver.pipeline_organizations_silver",
    comment="Validated healthcare organization reference data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
# CRITICAL: Expectations evaluate on OUTPUT schema (after .select())
@dp.expect_or_fail("valid_org_id", "organization_id IS NOT NULL")
@dp.expect_or_fail("valid_org_name", "organization_name IS NOT NULL")
@dp.expect_all({
    "valid_state": "state IS NOT NULL",
    "valid_city": "city IS NOT NULL",
    "complete_location": "latitude IS NOT NULL AND longitude IS NOT NULL"
})
def pipeline_organizations_silver():
    """
    Silver organization table with basic validation.
    
    Professional Exam Patterns:
    - expect_or_fail: Critical dimension keys (Id, NAME)
    - expect_all: Monitor reference data completeness
    - Reference dimension: Slowly changing, minimal transformation
    
    Why this approach?
    - Organizations are reference data (283 rows, stable)
    - Minimal transformation - just standardization
    - expect_or_fail on keys prevents downstream join issues
    """
    
    return (
        spark.readStream.table("healthcare.bronze.pipeline_organizations_bronze")
        # Standardize state codes to uppercase
        .withColumn("STATE", F.upper(F.col("STATE")))
        # Remove special characters from phone for consistency
        .withColumn(
            "phone_cleaned",
            F.regexp_replace(F.col("PHONE"), "[^0-9]", "")
        )
        # Flag high-utilization organizations (> 100 patients)
        .withColumn(
            "high_utilization_flag",
            F.when(F.col("UTILIZATION") > 100, True).otherwise(False)
        )
        # Select and rename columns
        .select(
            F.col("Id").alias("organization_id"),
            F.col("NAME").alias("organization_name"),
            F.col("ADDRESS").alias("address"),
            F.col("CITY").alias("city"),
            F.col("STATE").alias("state"),
            F.col("ZIP").alias("zip_code"),
            F.col("LAT").alias("latitude"),
            F.col("LON").alias("longitude"),
            F.col("phone_cleaned").alias("phone"),
            F.col("REVENUE").alias("revenue"),
            F.col("UTILIZATION").alias("utilization"),
            F.col("high_utilization_flag"),
            F.col("_ingestion_timestamp").alias("silver_timestamp")
        )
    )