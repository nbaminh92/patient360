# Patient360 Data Pipeline

Production-grade healthcare data pipeline demonstrating **Databricks Certified Data Engineer Professional** exam patterns.

## 🏗️ Architecture

**Multi-Hop Medallion Architecture** with production-grade data quality framework:

```
SOURCE (CSV) → BRONZE (Raw + Validation) → SILVER (Cleansed + Deduplicated) → MONITORING
```

### Pipeline Metadata
- **Pipeline ID**: `b6996970-42ed-4025-bde4-a4c3794384b4`
- **Unity Catalog**: `healthcare`
- **Status**: ✅ Production Ready with Daily Monitoring

## 📊 Data Flow

### Source Layer
| Dataset | Records | Path |
|---------|---------|------|
| Patients | 339 | `/databricks-datasets/rwe/ehr/csv/patients` |
| Organizations | 283 | `/databricks-datasets/rwe/ehr/csv/organizations` |
| Observations | 114,342 | `/databricks-datasets/rwe/ehr/csv/observations` |

### Bronze Layer (Raw Ingestion + Schema Validation)
**Table Type**: `STREAMING_TABLE` (all 3 tables)

**3-Tier Data Quality Framework**:
1. **@expect_or_fail** - Critical validations (stops pipeline)
2. **@expect_or_drop** - Row-level quarantine (isolates bad records)
3. **Monitoring** - Trend tracking without blocking (schema drift, freshness)

| Table | Records | Critical Expectations | Quarantine Expectations | Monitoring |
|-------|---------|----------------------|------------------------|------------|
| `pipeline_patients_bronze` | 339 | `valid_patient_id` | `valid_birthdate`, `valid_gender` | `birthdate_in_past`, `schema_drift` |
| `pipeline_organizations_bronze` | 283 | `valid_org_id` | `valid_org_name` | `non_negative_revenue`, `schema_drift` |
| `pipeline_observations_bronze` | 114,342 | `valid_patient_ref` | `valid_observation_date`, `valid_observation_code` | `has_observation_value`, `observation_in_past`, `schema_drift` |

**Current Status**: ✅ 0 schema drift violations detected

### Silver Layer (Cleansed + Deduplicated)

**Mixed Table Types** (optimized per use case):

| Table | Type | Records | Transformation | Why This Type? |
|-------|------|---------|----------------|----------------|
| `pipeline_patients_silver_batch` | **MATERIALIZED_VIEW** | 169 (50.15% dedup) | Window function deduplication | Requires complete dataset visibility to rank duplicates |
| `pipeline_organizations_silver` | **STREAMING_TABLE** | 283 | Data cleaning | Append-only, no deduplication needed |
| `pipeline_observations_silver` | **STREAMING_TABLE** | 114,342 | Data enrichment | Append-only, no deduplication needed |

**Deduplication Logic** (Patients):
```sql
ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _src_file_ts DESC) = 1
```

## 📈 Monitoring & Alerting

**Automated Daily Job**: `Daily Patient360 Pipeline Monitoring`
- **Job ID**: `1123543712011554`
- **Schedule**: Every 24 hours
- **Alerts**: Email to `nba.minh7892@gmail.com`

### Operational Metrics
1. ✅ **Schema Drift Detection** - `_rescued_data` monitoring (0 violations)
2. ⚠️ **Data Freshness SLAs** - Max lag: 6 hours (Current: 146h - WARNING)
3. ✅ **Expectation Failure Rates** - SLA: <5% (Within tolerance)
4. ✅ **Deduplication Metrics** - 50.15% reduction validated
5. ✅ **Record Count Validation** - 114,964 (Bronze) → 114,794 (Silver)
6. ⚠️ **Alert Severity** - 3 active warnings (freshness SLA exceeded)

## 🎓 Professional Exam Patterns Demonstrated

### 1. Data Processing and Storage (25%)
- ✅ Multi-hop medallion architecture (Bronze → Silver)
- ✅ Mixed table types (streaming + materialized views)
- ✅ Auto Loader pattern for CSV ingestion
- ✅ Schema evolution handling via `_rescued_data`
- ✅ Unity Catalog integration

### 2. Production Pipelines (30%)
- ✅ Three-tier expectation framework (@fail/@drop/monitor)
- ✅ Error handling via quarantine pattern
- ✅ Performance optimization via table type selection
- ✅ Transformation effectiveness tracking
- ✅ Cost-conscious design

### 3. Advanced Data Engineering (20%)
- ✅ Window function deduplication (`ROW_NUMBER()`)
- ✅ Incremental processing for append-only data
- ✅ Batch processing for aggregation/deduplication
- ✅ Schema drift detection and monitoring
- ✅ Complex transformation logic in Silver layer

### 4. Security and Governance (15%)
- ✅ Unity Catalog table organization
- ✅ Access control through Unity Catalog
- ✅ Data lineage tracking
- ✅ Audit trail via monitoring dashboard
- ✅ Email-based alerting for compliance

### 5. Monitoring and Operations (10%)
- ✅ Automated daily monitoring job
- ✅ SLA-based alerting (freshness, failure rates)
- ✅ Multi-signal operational metrics
- ✅ Proactive vs reactive monitoring
- ✅ Production troubleshooting framework

## 🚀 Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Access to `healthcare` catalog
- Sample data at `/databricks-datasets/rwe/ehr/csv/`

### Deployment

1. **Import Pipeline**:
   - Navigate to Workflows → Lakeflow Pipelines → Create Pipeline
   - Point to this repository's `transformations/` directory
   - Set target catalog: `healthcare`

2. **Configure Monitoring**:
   - Import `explorations/Pipeline_Monitoring_Dashboard.py` as notebook
   - Create daily job pointing to the notebook
   - Configure email alerts

3. **Run Pipeline**:
   - Start pipeline update
   - Monitor expectations in pipeline UI
   - Verify Bronze/Silver table creation

## 📂 Project Structure

```
patient360/
├── transformations/
│   ├── bronze/
│   │   ├── patients.py          # Bronze patients with 5 expectations
│   │   ├── organizations.py     # Bronze organizations with 4 expectations
│   │   └── observations.py      # Bronze observations with 6 expectations
│   └── silver/
│       ├── patients.py          # Silver patients (deduplication)
│       ├── organizations.py     # Silver organizations (cleaning)
│       └── observations.py      # Silver observations (enrichment)
└── explorations/
    └── Pipeline_Monitoring_Dashboard.py  # 6 operational metrics
```

## 📖 Key Concepts

### Why MATERIALIZED_VIEW for Patient Deduplication?
Deduplication requires visibility across the **entire dataset** to identify and rank duplicates using window functions. Streaming processing would handle records incrementally and miss duplicates across batches.

### Why STREAMING_TABLE for Organizations/Observations?
These are **append-only** datasets without deduplication requirements. Streaming tables provide lower latency and efficient incremental processing for cleaning and validation transformations.

### Schema Drift Detection
Monitor the `_rescued_data` column in Bronze tables. When row count > 0, upstream schema has changed and new columns are being captured. This provides early warning without blocking ingestion.

### Three-Tier Expectation Strategy
- **@expect_or_fail**: Critical structural validations (e.g., `Id IS NOT NULL`) - stops pipeline
- **@expect_or_drop**: Row-level data quality (e.g., `BIRTHDATE IS NOT NULL`) - quarantines bad records
- **Monitoring**: Trend tracking (e.g., schema drift, freshness) - alerts without blocking

## 🔍 Sample Queries

### Check Bronze Data Quality
```sql
-- Check for schema drift
SELECT COUNT(*) as drift_count 
FROM healthcare.bronze.pipeline_patients_bronze 
WHERE _rescued_data IS NOT NULL;

-- View quarantined records (failed expect_or_drop)
SELECT * 
FROM healthcare.bronze.pipeline_patients_bronze 
WHERE BIRTHDATE IS NULL;
```

### Verify Silver Deduplication
```sql
-- Patients deduplication effectiveness
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT Id) as unique_patients,
  COUNT(*) - COUNT(DISTINCT Id) as duplicates_removed,
  ROUND((COUNT(*) - COUNT(DISTINCT Id)) / COUNT(*) * 100, 2) as dedup_percentage
FROM healthcare.bronze.pipeline_patients_bronze;

-- Compare Bronze vs Silver record counts
SELECT 'Bronze' as layer, COUNT(*) as records 
FROM healthcare.bronze.pipeline_patients_bronze
UNION ALL
SELECT 'Silver' as layer, COUNT(*) as records 
FROM healthcare.silver.pipeline_patients_silver_batch;
```

### Monitor Data Freshness
```sql
-- Check when data was last ingested
SELECT 
  MAX(_ingestion_timestamp) as last_ingestion,
  DATEDIFF(HOUR, MAX(_ingestion_timestamp), CURRENT_TIMESTAMP()) as hours_since_last_update
FROM healthcare.bronze.pipeline_patients_bronze;
```

## 🎯 Exam Scenario Example

**Question**: *"Your healthcare pipeline ingests patient data from multiple sources. Some records are duplicates, some have missing required fields, and the upstream schema occasionally changes. Design a production pipeline with appropriate data quality controls, monitoring, and alerting."*

**Answer (This Architecture)**:
1. **Bronze Layer**: Streaming ingestion with @expect_or_fail for required fields, @expect_or_drop for quality issues, monitoring for schema drift via `_rescued_data`
2. **Silver Layer**: Materialized view for deduplication (window functions require full dataset), streaming tables for append-only transformations
3. **Schema Evolution**: Monitor `_rescued_data` column (0 violations = no drift)
4. **Monitoring**: Daily job tracking 6 metrics with severity-based alerting
5. **Operations**: Email alerts, automated scheduling, actionable remediation steps

## 📚 References

- [Databricks Certified Data Engineer Professional](https://www.databricks.com/learn/certification/data-engineer-professional)
- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake](https://docs.delta.io/)

## 📧 Contact

**Owner**: nba.minh7892@gmail.com  
**Status**: Production Ready  
**Last Updated**: 2026-08-21
