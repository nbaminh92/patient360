# Silver Layer - Clinical Observations
# Pattern: Type conversion + Data Quality + Referential Integrity
# Professional Exam Focus: Parsing string to numeric, quality expectations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="healthcare.silver.pipeline_observations_silver",
    comment="Validated clinical observations with parsed numeric values",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
# CRITICAL: Expectations evaluate on OUTPUT schema (after .select())
@dp.expect_or_fail("valid_patient_reference", "patient_id IS NOT NULL")
@dp.expect_or_drop("valid_observation_date", "observation_timestamp IS NOT NULL AND observation_timestamp <= CURRENT_TIMESTAMP()")
@dp.expect_all({
    "has_category": "category IS NOT NULL",
    "has_code": "loinc_code IS NOT NULL",
    "has_description": "description IS NOT NULL"
})
@dp.expect("numeric_values_present", "value_type != 'numeric' OR (value_type = 'numeric' AND value_raw IS NOT NULL)")
def pipeline_observations_silver():
    """
    Silver observations table with type conversion and quality validation.
    
    Professional Exam Patterns:
    - Type parsing: Convert VALUE string to numeric where TYPE = 'numeric'
    - Referential integrity: Expect valid PATIENT reference
    - expect_or_drop: Filter out invalid/future-dated observations
    - expect: Monitor numeric value completeness without dropping
    
    Why this approach?
    - VALUE column is STRING in Bronze (came from CSV)
    - Need numeric parsing for aggregations in Gold layer
    - Keep both raw VALUE and parsed numeric_value for audit trail
    """
    
    return (
        spark.readStream.table("healthcare.bronze.pipeline_observations_bronze")
        # Parse VALUE to numeric when TYPE = 'numeric'
        .withColumn(
            "numeric_value",
            F.when(
                F.col("TYPE") == "numeric",
                F.col("VALUE").cast("double")
            ).otherwise(None)
        )
        # Standardize category names to lowercase
        .withColumn("CATEGORY", F.lower(F.col("CATEGORY")))
        # Extract date portion from timestamp for easy filtering
        .withColumn("observation_date", F.to_date(F.col("DATE")))
        # Flag vital signs vs lab results for downstream processing
        .withColumn(
            "observation_type",
            F.when(F.col("CATEGORY").contains("vital"), "vital_signs")
            .when(F.col("CATEGORY").contains("lab"), "laboratory")
            .otherwise("other")
        )
        # Select and rename columns
        .select(
            F.col("DATE").alias("observation_timestamp"),
            F.col("observation_date"),
            F.col("PATIENT").alias("patient_id"),
            F.col("ENCOUNTER").alias("encounter_id"),
            F.col("CATEGORY").alias("category"),
            F.col("observation_type"),
            F.col("CODE").alias("loinc_code"),
            F.col("DESCRIPTION").alias("description"),
            F.col("VALUE").alias("value_raw"),
            F.col("numeric_value"),
            F.col("UNITS").alias("units"),
            F.col("TYPE").alias("value_type"),
            F.col("_ingestion_timestamp").alias("silver_timestamp")
        )
    )