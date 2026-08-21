# Silver Layer - Patient Demographics
# Pattern: Deduplication + Data Quality Expectations
# Professional Exam Focus: Latest record selection, quality validation

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.table(
    name="healthcare.silver.pipeline_patients_silver_batch",
    comment="Deduplicated and validated patient demographics with latest record per patient",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
# CRITICAL: Expectations evaluate on OUTPUT schema (after .select())
@dp.expect_or_fail("valid_patient_id", "patient_id IS NOT NULL")
@dp.expect_or_drop("valid_birthdate_range", "birth_date >= '1900-01-01' AND birth_date <= CURRENT_DATE()")
@dp.expect_all({
    "has_name": "first_name IS NOT NULL AND last_name IS NOT NULL",
    "valid_gender": "gender IN ('M', 'F')",
    "valid_state": "state IS NOT NULL"
})
def pipeline_patients_silver():
    """
    Silver patient table with deduplication and quality validation.
    
    Professional Exam Patterns:
    - Deduplication using window functions (latest record per patient)
    - expect_or_fail: Critical business keys (Id)
    - expect_or_drop: Data quality filtering (invalid birthdates)
    - expect_all: Monitoring quality metrics
    
    Why this approach?
    - 3 patient snapshot files suggest updates over time
    - Keep latest record per patient based on ingestion timestamp
    - Alternative would be SCD Type 2 (track all changes) - overkill for snapshots
    """
    
    # Read bronze patients as BATCH (not streaming)
    # Why? ROW_NUMBER() deduplication requires seeing all records per partition
    # This is a common exam trap: window functions need batch semantics
    bronze_patients = spark.read.table("healthcare.bronze.pipeline_patients_bronze")
    
    # Define window to get latest record per patient
    # Partition by patient Id, order by ingestion timestamp descending
    window_spec = Window.partitionBy("Id").orderBy(F.col("_ingestion_timestamp").desc())
    
    return (
        bronze_patients
        # Add row number to identify latest record
        .withColumn("row_num", F.row_number().over(window_spec))
        # Keep only latest record per patient
        .filter(F.col("row_num") == 1)
        # Calculate age from birthdate
        .withColumn(
            "age",
            F.floor(F.months_between(F.current_date(), F.col("BIRTHDATE")) / 12)
        )
        # Standardize state codes to uppercase
        .withColumn("STATE", F.upper(F.col("STATE")))
        # Create full name for convenience
        .withColumn(
            "full_name",
            F.concat_ws(" ", F.col("PREFIX"), F.col("FIRST"), F.col("MIDDLE"), F.col("LAST"))
        )
        # Select and rename columns for Silver schema
        .select(
            F.col("Id").alias("patient_id"),
            F.col("full_name"),
            F.col("FIRST").alias("first_name"),
            F.col("LAST").alias("last_name"),
            F.col("GENDER").alias("gender"),
            F.col("BIRTHDATE").alias("birth_date"),
            F.col("age"),
            F.col("RACE").alias("race"),
            F.col("ETHNICITY").alias("ethnicity"),
            F.col("MARITAL").alias("marital_status"),
            F.col("ADDRESS").alias("address"),
            F.col("CITY").alias("city"),
            F.col("STATE").alias("state"),
            F.col("ZIP").alias("zip_code"),
            F.col("LAT").alias("latitude"),
            F.col("LON").alias("longitude"),
            F.col("HEALTHCARE_EXPENSES").alias("healthcare_expenses"),
            F.col("HEALTHCARE_COVERAGE").alias("healthcare_coverage"),
            F.col("INCOME").alias("income"),
            F.col("_ingestion_timestamp").alias("silver_timestamp"),
            F.col("_source_file").alias("source_file")
        )
    )