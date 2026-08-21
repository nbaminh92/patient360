# Patient360 Pipeline Architecture

## System Design

### High-Level Flow
```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SOURCE DATA LAYER                              │
│                                                                           │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │  Patients    │  │  Organizations   │  │     Observations         │  │
│  │  (CSV)       │  │  (CSV)          │  │     (CSV)                │  │
│  │  339 records │  │  283 records    │  │     114,342 records      │  │
│  └──────┬───────┘  └────────┬─────────┘  └──────────┬───────────────┘  │
└─────────┼────────────────────┼────────────────────────┼──────────────────┘
          │                    │                        │
          │ Auto Loader        │ Auto Loader            │ Auto Loader
          ▼                    ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (RAW + VALIDATION)                  │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ STREAMING_TABLE: pipeline_patients_bronze                          │ │
│  │ • @expect_or_fail: valid_patient_id                                │ │
│  │ • @expect_or_drop: valid_birthdate, valid_gender                   │ │
│  │ • Monitoring: birthdate_in_past, schema_drift                      │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ STREAMING_TABLE: pipeline_organizations_bronze                     │ │
│  │ • @expect_or_fail: valid_org_id                                    │ │
│  │ • @expect_or_drop: valid_org_name                                  │ │
│  │ • Monitoring: non_negative_revenue, schema_drift                   │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ STREAMING_TABLE: pipeline_observations_bronze                      │ │
│  │ • @expect_or_fail: valid_patient_ref                               │ │
│  │ • @expect_or_drop: valid_observation_date, valid_observation_code  │ │
│  │ • Monitoring: has_observation_value, observation_in_past, drift    │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────┬──────────────────────┬─────────────────────────┬──────────────┘
          │                      │                         │
          │ Transformation       │ Transformation          │ Transformation
          ▼                      ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER (CLEANSED + DEDUPLICATED)              │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ MATERIALIZED_VIEW: pipeline_patients_silver_batch                  │ │
│  │ • Deduplication via ROW_NUMBER() OVER (PARTITION BY Id)            │ │
│  │ • 339 → 169 records (50.15% reduction)                             │ │
│  │ • All expectations PASSED ✓                                        │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ STREAMING_TABLE: pipeline_organizations_silver                     │ │
│  │ • Data cleaning (name trimming, revenue validation)                │ │
│  │ • 283 records                                                      │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ STREAMING_TABLE: pipeline_observations_silver                      │ │
│  │ • Data enrichment (date validation, code normalization)            │ │
│  │ • 114,342 records                                                  │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────┬──────────────────────┬─────────────────────────┬──────────────┘
          │                      │                         │
          └──────────────────────┴─────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    MONITORING & ALERTING LAYER                           │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Daily Job: Pipeline_Monitoring_Dashboard                           │ │
│  │ • Schema Drift Detection (0 violations)                            │ │
│  │ • Data Freshness SLAs (6h threshold)                               │ │
│  │ • Expectation Failure Rates (<5% SLA)                              │ │
│  │ • Deduplication Metrics (50.15% validated)                         │ │
│  │ • Record Count Validation (Bronze→Silver)                          │ │
│  │ • Alert Severity (CRITICAL vs WARNING)                             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  Email Alerts → nba.minh7892@gmail.com                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Design Decisions

### Table Type Selection

#### MATERIALIZED_VIEW for Patients (Silver)
**Decision**: Use MATERIALIZED_VIEW instead of STREAMING_TABLE

**Rationale**:
- Deduplication requires complete dataset visibility
- Window functions (`ROW_NUMBER() OVER PARTITION BY`) need to see all records to correctly rank duplicates
- Streaming processing is incremental and would miss duplicates across micro-batches
- Performance: One-time batch processing vs. multiple streaming checkpoints

**Trade-offs**:
- ✅ Accurate deduplication (50.15% reduction validated)
- ✅ Simple to reason about (batch semantics)
- ❌ Higher latency (full refresh vs. incremental)
- ❌ Not suitable for real-time requirements

#### STREAMING_TABLE for Organizations/Observations (Silver)
**Decision**: Use STREAMING_TABLE for append-only transformations

**Rationale**:
- No deduplication requirements
- Append-only data (new observations, no updates)
- Lower latency (incremental processing)
- Cost-efficient (process only new data)

**Trade-offs**:
- ✅ Low latency (real-time processing)
- ✅ Cost-efficient (incremental checkpointing)
- ✅ Scalable for high-volume data
- ❌ Cannot perform full-table aggregations easily

### Expectation Strategy

#### Three-Tier Framework
1. **@expect_or_fail**: Pipeline-stopping validations
   - Use for: Structural integrity (PRIMARY KEY not null)
   - Impact: Stops entire pipeline on violation
   - Example: `Id IS NOT NULL`

2. **@expect_or_drop**: Row-level quarantine
   - Use for: Data quality issues (bad dates, invalid enum values)
   - Impact: Drops/quarantines bad rows, pipeline continues
   - Example: `BIRTHDATE IS NOT NULL`

3. **Monitoring**: Trend tracking
   - Use for: Non-blocking quality metrics
   - Impact: No pipeline impact, generates metrics
   - Example: `_rescued_data IS NULL` (schema drift detection)

#### Why This Mix?
- **Availability**: Pipeline keeps running despite data quality issues
- **Data Quality**: Bad records are isolated, not propagated
- **Observability**: Trends visible without blocking
- **Professional Exam**: Demonstrates production-grade error handling

## Schema Evolution Strategy

### _rescued_data Column
Auto Loader creates `_rescued_data` column to capture schema changes:

```sql
-- Monitor for schema drift
SELECT COUNT(*) as drift_violations
FROM healthcare.bronze.pipeline_patients_bronze
WHERE _rescued_data IS NOT NULL;
```

**Benefits**:
- No pipeline failures on schema changes
- Early warning system
- Captured data preserved for analysis
- Professional exam pattern: graceful degradation

## Performance Optimizations

### Bronze Layer
- **STREAMING_TABLE**: Continuous ingestion, minimal latency
- **Auto Loader**: Incremental file discovery, optimized for cloud storage
- **Expectations**: Lightweight validations (no complex joins)

### Silver Layer
- **Patients (Batch)**: One-time deduplication, not time-critical
- **Organizations/Observations (Streaming)**: Real-time transformations
- **Selective Projections**: Only necessary columns in Silver

## Monitoring Architecture

### 6 Operational Metrics
1. **Schema Drift**: `_rescued_data` row counts
2. **Data Freshness**: `_ingestion_timestamp` lag vs. SLA
3. **Expectation Failures**: Violation rate vs. <5% threshold
4. **Deduplication Effectiveness**: Bronze→Silver reduction %
5. **Record Counts**: Layer-to-layer reconciliation
6. **Alert Severity**: CRITICAL (>5% failures) vs. WARNING (freshness)

### Alert Routing
- **Email**: Immediate notification for pipeline failures
- **Dashboard**: Historical trends, SLA compliance
- **Actionable**: Each alert includes remediation steps

## Cost Optimization

### Storage
- **Bronze**: Raw data, compressed Delta format
- **Silver**: Deduplicated, optimized for queries
- **No Gold Layer Yet**: Avoid premature optimization

### Compute
- **Streaming**: Runs continuously, but only processes new data
- **Batch (Patients)**: Runs on-demand, full refresh acceptable
- **Monitoring Job**: Daily (not hourly), sufficient for SLA

## Security & Governance

### Unity Catalog Integration
- **Catalog**: `healthcare` (PHI data)
- **Schema Separation**: `bronze` (raw) vs. `silver` (processed)
- **Audit Trail**: Monitoring dashboard provides change tracking

### Compliance
- **Data Quality**: Ensures accuracy for regulatory reporting
- **Lineage**: Bronze→Silver→Monitoring tracked automatically
- **Alerting**: Email notifications provide audit trail

## Future Enhancements

### Gold Layer
- Patient 360° view (joins patients + observations + organizations)
- Aggregated metrics for analytics
- Optimized for BI tool queries

### Advanced Monitoring
- Query system.lakeflow.data_quality_events when available
- Integration with PagerDuty/Slack
- Cost tracking per pipeline run

### CI/CD Integration
- GitHub Actions for automated testing
- Branch protection (main = production)
- Automated deployment via Databricks Asset Bundles

## Professional Exam Relevance

This architecture demonstrates ALL five exam sections:

1. **Data Processing & Storage (25%)**: Multi-hop medallion, mixed table types, Auto Loader, Unity Catalog
2. **Production Pipelines (30%)**: Three-tier expectations, error handling, performance optimization
3. **Advanced Data Engineering (20%)**: Window functions, incremental processing, schema evolution
4. **Security & Governance (15%)**: Unity Catalog, lineage, audit trails
5. **Monitoring & Operations (10%)**: Automated monitoring, SLA alerting, troubleshooting framework

**Exam Scenario Coverage**: This architecture directly answers scenario-based questions about building production healthcare pipelines with data quality requirements.
