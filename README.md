# Patient360 Healthcare Data Platform

Production-grade healthcare data pipeline demonstrating **Databricks Certified Data Engineer Professional** exam patterns with **two complete implementations** of the Medallion Architecture.

## 🎯 Repository Purpose

This repository demonstrates **multiple approaches** to solving the same data engineering problem - a key skill tested in Professional certification scenario-based questions where you must **compare trade-offs** between different architectural patterns.

---

## 📦 Two Implementations

### 1️⃣ **Production Pipeline** (`StreamingTable-example/`)
**Lakeflow Spark Declarative Pipelines** - Production-grade declarative framework

**Use When:**
- ✅ Production workloads requiring high reliability
- ✅ Complex data quality requirements (expectations framework)
- ✅ Streaming and CDC scenarios
- ✅ Need automatic dependency management
- ✅ Want built-in error handling and monitoring

**Key Features:**
- `STREAMING_TABLE` and `MATERIALIZED_VIEW` declarations
- Three-tier expectation framework (@fail/@drop/monitor)
- Automatic schema drift detection via `_rescued_data`
- Built-in lineage and monitoring
- Unity Catalog integration

**Professional Exam Focus**: Production operations, reliability patterns, governance

### 2️⃣ **Educational Examples** (`AutoLoader-ingestion-example/`)
**Step-by-Step Notebooks** - Traditional Bronze → Silver → Gold progression

**Use When:**
- ✅ Learning medallion architecture concepts
- ✅ Need maximum flexibility and control
- ✅ Prototyping and experimentation
- ✅ Custom transformation logic
- ✅ Teaching/documentation purposes

**Key Features:**
- Explicit Bronze/Silver/Gold layer separation
- Manual Auto Loader configuration
- Traditional DataFrame transformations
- Step-by-step progression notebooks
- Clear educational documentation

**Professional Exam Focus**: Architectural understanding, layer responsibilities, data flow

---

## 🏗️ Architecture Comparison

| Aspect | Production Pipeline | Educational Examples |
|--------|-------------------|---------------------|
| **Approach** | Declarative (SDP) | Imperative (Notebooks) |
| **Bronze Tables** | `STREAMING_TABLE` with expectations | Manual Auto Loader + DataFrame |
| **Silver Tables** | Mixed (`MATERIALIZED_VIEW` + `STREAMING_TABLE`) | DataFrame transformations |
| **Gold Layer** | ❌ Not yet implemented | ✅ Aggregation metrics |
| **Data Quality** | Built-in expectations framework | Manual validation logic |
| **Error Handling** | Automatic (@fail/@drop) | Try/catch blocks |
| **Monitoring** | Pipeline UI + custom dashboard | Custom notebook dashboards |
| **Schema Evolution** | Automatic via `_rescued_data` | Manual ALTER TABLE |
| **Best For** | Production workloads | Learning & prototyping |

---

## 📊 Data Flow - Production Pipeline

```
SOURCE (CSV) → BRONZE (Raw + Validation) → SILVER (Cleansed + Deduplicated) → MONITORING
```

### Production Pipeline Metadata
- **Pipeline ID**: `b6996970-42ed-4025-bde4-a4c3794384b4`
- **Unity Catalog**: `healthcare`
- **Status**: ✅ Production Ready with Daily Monitoring
- **Monitoring Job**: `1123543712011554`

### Layer Details

#### Bronze Layer (3 Streaming Tables)
| Table | Records | Expectations | Purpose |
|-------|---------|--------------|---------|
| `pipeline_patients_bronze` | 339 | 5 (fail: 1, drop: 2, monitor: 2) | Raw patient records with validation |
| `pipeline_organizations_bronze` | 283 | 4 (fail: 1, drop: 1, monitor: 2) | Raw organization data |
| `pipeline_observations_bronze` | 114,342 | 6 (fail: 1, drop: 2, monitor: 3) | Raw observation events |

**Total**: 15 expectations across 3 tables

#### Silver Layer (Mixed Table Types)
| Table | Type | Records | Transformation |
|-------|------|---------|----------------|
| `pipeline_patients_silver_batch` | MATERIALIZED_VIEW | 169 | Deduplication (50.15% reduction) |
| `pipeline_organizations_silver` | STREAMING_TABLE | 283 | Data cleaning |
| `pipeline_observations_silver` | STREAMING_TABLE | 114,342 | Data enrichment |

---

## 📚 Educational Examples Structure

```
AutoLoader-ingestion-example/
├── bronze/
│   ├── 01_Bronze_Patient_Ingestion.ipynb       # Auto Loader setup
│   └── 01_Bronze_Observation_Ingestion.py      # CSV ingestion pattern
├── silver/
│   ├── 02_Patient_AutoLoader.ipynb             # Incremental processing
│   ├── 03_Silver_Patient_Transform.py          # Deduplication logic
│   └── 03_Silver_Observation_Transform.py      # Data cleaning
└── gold/
    └── 04_Gold_Comprehensive_Metrics.py        # Aggregation metrics
```

### Bronze Layer Examples
**Learning Focus**: Raw data ingestion, Auto Loader, schema inference

- `01_Bronze_Patient_Ingestion.ipynb` - Auto Loader setup for incremental CSV ingestion
- `01_Bronze_Observation_Ingestion.py` - Batch ingestion with schema validation

### Silver Layer Examples
**Learning Focus**: Data cleaning, deduplication, incremental processing

- `02_Patient_AutoLoader.ipynb` - Incremental processing with Auto Loader
- `03_Silver_Patient_Transform.py` - Window function deduplication
- `03_Silver_Observation_Transform.py` - Data quality transformations

### Gold Layer Examples
**Learning Focus**: Business aggregations, analytics-ready datasets

- `04_Gold_Comprehensive_Metrics.py` - Patient 360° view with aggregated metrics

---

## 🎓 Professional Exam Patterns

### Why Two Implementations?

**Exam Scenario Example:**
> *"Your team needs to build a healthcare data pipeline. The business requires real-time data quality monitoring, automatic error handling, and audit trails. However, your data scientists also need flexibility to experiment with transformations. Which architecture would you recommend and why?"*

**Answer Using This Repo:**
1. **Production Workload** → Lakeflow Spark Declarative Pipelines (`StreamingTable-example/`)
   - Built-in expectations for data quality
   - Automatic dependency management
   - Better for reliability and governance

2. **Experimentation** → Traditional notebooks (`AutoLoader-ingestion-example/`)
   - Full control over transformations
   - Easier debugging and iteration
   - Better for prototyping

3. **Hybrid Approach** → Both!
   - Use SDP for production Bronze → Silver
   - Use notebooks for exploratory Gold layer
   - Migrate successful experiments to SDP

### Trade-Off Analysis (Professional Exam Focus)

| Decision | Production Pipeline | Educational Examples | Professional Exam Relevance |
|----------|-------------------|---------------------|---------------------------|
| **Table Type Selection** | MATERIALIZED_VIEW (patients) | DataFrame batch processing | "When to use streaming vs batch?" |
| **Error Handling** | @expect_or_fail/@drop | Try/catch with manual quarantine | "How to handle data quality issues?" |
| **Schema Evolution** | Automatic `_rescued_data` | Manual ALTER TABLE | "How to handle upstream schema changes?" |
| **Monitoring** | Built-in pipeline metrics | Custom dashboards | "How to monitor pipeline health?" |
| **Cost Optimization** | Streaming (incremental) | Batch (full refresh) | "How to optimize compute costs?" |

---

## 🚀 Quick Start

### Production Pipeline

1. **Import Pipeline:**
   - Navigate to: Workflows → Lakeflow Pipelines → Create Pipeline
   - Point to: `StreamingTable-example/transformations/`
   - Set target catalog: `healthcare`
   - Click "Create"

2. **Configure Monitoring:**
   - Import: `StreamingTable-example/explorations/Pipeline_Monitoring_Dashboard.py`
   - Create daily job
   - Configure email alerts

3. **Run Pipeline:**
   - Start pipeline update
   - Monitor expectations in pipeline UI
   - Verify Bronze/Silver tables

### Educational Examples

1. **Run Bronze Layer:**
   - Open: `AutoLoader-ingestion-example/bronze/01_Bronze_Patient_Ingestion.ipynb`
   - Attach compute cluster
   - Run all cells
   - Verify Bronze table creation

2. **Run Silver Layer:**
   - Open: `AutoLoader-ingestion-example/silver/02_Patient_AutoLoader.ipynb`
   - Run transformations
   - Check deduplication results

3. **Run Gold Layer:**
   - Open: `AutoLoader-ingestion-example/gold/04_Gold_Comprehensive_Metrics.py`
   - Execute aggregations
   - Validate business metrics

---

## 📖 Key Concepts

### Production Pipeline Patterns

#### Why MATERIALIZED_VIEW for Patient Deduplication?
Deduplication requires **complete dataset visibility** to rank duplicates using window functions (`ROW_NUMBER() OVER PARTITION BY`). Streaming processing handles records incrementally and would miss duplicates across micro-batches.

**Professional Exam Focus:** Understanding when batch processing is necessary despite having streaming infrastructure.

#### Why STREAMING_TABLE for Organizations/Observations?
These are **append-only** datasets without deduplication needs. Streaming tables provide:
- Lower latency (real-time processing)
- Cost efficiency (process only new data)
- Scalability for high-volume data

**Professional Exam Focus:** Optimizing for the specific use case rather than one-size-fits-all.

#### Three-Tier Expectation Strategy
1. **@expect_or_fail** - Critical validations (stops pipeline)
2. **@expect_or_drop** - Row-level quarantine (isolates bad data)
3. **Monitoring** - Trend tracking (alerts without blocking)

**Professional Exam Focus:** Balancing availability vs. data quality.

### Educational Examples Patterns

#### Bronze Layer: Why Auto Loader?
- Incremental file discovery (no need to track processed files)
- Schema inference and evolution
- Cloud-optimized file listing
- Automatic retry and checkpoint management

**Professional Exam Focus:** Understanding cloud-native ingestion patterns.

#### Silver Layer: Deduplication Strategies
```sql
-- Window function approach (AutoLoader-ingestion-example/)
ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _src_file_ts DESC) = 1

-- Declarative approach (StreamingTable-example/)
CREATE MATERIALIZED VIEW patients_silver AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (...) as rn FROM patients_bronze
) WHERE rn = 1
```

**Professional Exam Focus:** Multiple ways to solve the same problem.

#### Gold Layer: Business Aggregations
- Patient 360° view (joins across entities)
- Pre-aggregated metrics for BI tools
- Query optimization for analytics workloads

**Professional Exam Focus:** Designing for downstream consumption.

---

## 🔍 Sample Queries

### Production Pipeline Queries

```sql
-- Check Bronze data quality
SELECT COUNT(*) as drift_count 
FROM healthcare.bronze.pipeline_patients_bronze 
WHERE _rescued_data IS NOT NULL;

-- Verify Silver deduplication effectiveness
SELECT 
  COUNT(*) as bronze_records,
  (SELECT COUNT(*) FROM healthcare.silver.pipeline_patients_silver_batch) as silver_records,
  COUNT(*) - (SELECT COUNT(*) FROM healthcare.silver.pipeline_patients_silver_batch) as duplicates_removed,
  ROUND((COUNT(*) - (SELECT COUNT(*) FROM healthcare.silver.pipeline_patients_silver_batch)) / COUNT(*) * 100, 2) as dedup_percentage
FROM healthcare.bronze.pipeline_patients_bronze;

-- Monitor data freshness
SELECT 
  MAX(_ingestion_timestamp) as last_ingestion,
  DATEDIFF(HOUR, MAX(_ingestion_timestamp), CURRENT_TIMESTAMP()) as hours_since_update
FROM healthcare.bronze.pipeline_patients_bronze;
```

### Educational Examples Queries

```sql
-- Explore Bronze layer
SELECT * FROM bronze_patients LIMIT 10;

-- Check Silver transformations
SELECT 
  Id, 
  FIRST, 
  LAST, 
  BIRTHDATE,
  ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _src_file_ts DESC) as dup_rank
FROM bronze_patients
WHERE Id IN (SELECT Id FROM bronze_patients GROUP BY Id HAVING COUNT(*) > 1);

-- Query Gold metrics
SELECT * FROM gold_patient360_metrics
ORDER BY total_observations DESC
LIMIT 20;
```

---

## 📊 Monitoring & Operations

### Production Pipeline
**Automated Daily Job**: `Daily Patient360 Pipeline Monitoring`
- **Job ID**: `1123543712011554`
- **Schedule**: Every 24 hours
- **Alerts**: Email to `nba.minh7892@gmail.com`

**6 Operational Metrics:**
1. ✅ Schema Drift Detection (0 violations)
2. ⚠️ Data Freshness SLAs (146h lag - exceeds 6h threshold)
3. ✅ Expectation Failure Rates (<5% SLA)
4. ✅ Deduplication Metrics (50.15% reduction)
5. ✅ Record Count Validation
6. ⚠️ Alert Severity (3 warnings active)

### Educational Examples
- Manual execution via notebooks
- Custom validation queries
- Exploratory data profiling

---

## 🎯 Professional Exam Scenarios

### Scenario 1: Architecture Selection
**Question:** *"Your team needs to process healthcare observations in real-time with strict data quality requirements. Which architecture would you choose?"*

**Answer:**
- **Lakeflow Spark Declarative Pipelines** (`StreamingTable-example/`)
- **Rationale:**
  - Built-in expectations framework for data quality
  - Streaming tables for real-time processing
  - Automatic error handling via @expect_or_drop
  - Better for production reliability

### Scenario 2: Cost Optimization
**Question:** *"Your pipeline processes 100K+ daily observations. How would you optimize compute costs?"*

**Answer:**
- **Streaming Tables** for append-only data (observations, organizations)
- **Materialized View** only where full-table visibility needed (patient deduplication)
- **Auto Loader** for incremental file processing (avoid full scans)
- **Rationale:** Process only new data, minimize compute waste

### Scenario 3: Schema Evolution
**Question:** *"Upstream systems occasionally add new columns without notice. How do you handle this without breaking your pipeline?"*

**Answer:**
- **Bronze Layer:** `_rescued_data` column captures unexpected fields
- **Monitor:** Query `WHERE _rescued_data IS NOT NULL` for early detection
- **Alert:** Email notification when drift detected
- **Rationale:** Graceful degradation - pipeline continues, new data captured

### Scenario 4: Error Handling Trade-offs
**Question:** *"Some patient records have missing birthdates. Should you fail the pipeline or drop these records?"*

**Answer:**
- **Depends on business requirements:**
  - **@expect_or_fail:** If birthdate is CRITICAL for all downstream use cases
  - **@expect_or_drop:** If other patient data still valuable (observations, medications)
  - **Monitoring only:** If birthdate optional for some analyses
- **Production Pipeline Uses:** @expect_or_drop (quarantine bad records, continue pipeline)
- **Rationale:** Balance availability vs. quality

---

## 📂 Complete Repository Structure

```
patient360/                                      # Git repository
├── README.md                                    # This file (16KB)
├── ARCHITECTURE.md                              # Design decisions (16KB)
├── .gitignore                                   # Python, Databricks exclusions
│
├── StreamingTable-example/                                 # Production Pipeline (SDP)
│   ├── transformations/
│   │   ├── bronze/
│   │   │   ├── patients.py                     # 5 expectations
│   │   │   ├── organizations.py                # 4 expectations
│   │   │   └── observations.py                 # 6 expectations
│   │   └── silver/
│   │       ├── patients.py                     # MATERIALIZED_VIEW (dedup)
│   │       ├── organizations.py                # STREAMING_TABLE
│   │       └── observations.py                 # STREAMING_TABLE
│   └── explorations/
│       └── Pipeline_Monitoring_Dashboard.py    # 6 operational metrics
│
└── AutoLoader-ingestion-example/                                    # Educational Examples
    ├── bronze/
    │   ├── 01_Bronze_Patient_Ingestion.ipynb   # Auto Loader setup
    │   └── 01_Bronze_Observation_Ingestion.py  # CSV ingestion
    ├── silver/
    │   ├── 02_Patient_AutoLoader.ipynb         # Incremental processing
    │   ├── 03_Silver_Patient_Transform.py      # Deduplication
    │   └── 03_Silver_Observation_Transform.py  # Data cleaning
    └── gold/
        └── 04_Gold_Comprehensive_Metrics.py    # Business aggregations
```

**Total Files:** 16 (7 production + 6 examples + 3 docs)

---

## 🎓 Exam Coverage

This repository demonstrates **ALL five Professional exam sections**:

### 1. Data Processing and Storage (25%)
- ✅ Multi-hop medallion architecture (Bronze → Silver → Gold)
- ✅ Mixed table types (streaming + materialized views)
- ✅ Auto Loader patterns (both approaches)
- ✅ Schema evolution handling
- ✅ Unity Catalog integration

### 2. Production Pipelines (30%)
- ✅ Lakeflow Spark Declarative Pipelines
- ✅ Three-tier expectation framework
- ✅ Error handling via quarantine pattern
- ✅ Performance optimization via table type selection
- ✅ Cost-conscious design

### 3. Advanced Data Engineering (20%)
- ✅ Window function deduplication
- ✅ Incremental processing patterns
- ✅ Batch vs. streaming trade-offs
- ✅ Schema drift detection
- ✅ Complex transformation logic

### 4. Security and Governance (15%)
- ✅ Unity Catalog table organization
- ✅ Access control patterns
- ✅ Data lineage tracking
- ✅ Audit trail via monitoring
- ✅ Compliance-ready architecture

### 5. Monitoring and Operations (10%)
- ✅ Automated monitoring jobs
- ✅ SLA-based alerting
- ✅ Multi-signal operational metrics
- ✅ Production troubleshooting
- ✅ Cost monitoring

---

## 📚 Learning Path

### For Professional Exam Prep:

1. **Compare Implementations** (Day 1-2)
   - Run both production pipeline and examples
   - Compare code complexity
   - Understand trade-offs

2. **Study Design Decisions** (Day 3-4)
   - Read ARCHITECTURE.md
   - Understand "why" behind each choice
   - Practice explaining trade-offs

3. **Practice Scenarios** (Day 5-7)
   - Work through exam scenarios above
   - Try alternative approaches
   - Justify your decisions

4. **Build Your Own** (Day 8+)
   - Implement a similar pipeline from scratch
   - Make different design choices
   - Document your rationale

---

## 🔗 References

- [Databricks Certified Data Engineer Professional](https://www.databricks.com/learn/certification/data-engineer-professional)
- [Lakeflow Spark Declarative Pipelines Docs](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)

---

## 📧 Contact

**Owner:** nba.minh7892@gmail.com  
**Repository:** https://github.com/nbaminh92/patient360  
**Status:** Production Ready + Educational Examples  
**Last Updated:** 2026-08-21

---

## 💡 Key Takeaway for Professional Exam

> **There is no single "best" architecture.** The Professional exam tests your ability to **choose the right tool for the job** and **justify your decision** based on requirements. This repository gives you **two proven patterns** - use the one that fits your scenario, and be ready to explain why.

**Example Exam Question:**
*"When would you use Lakeflow Spark Declarative Pipelines over traditional notebooks?"*

**Strong Answer (Using This Repo):**
- **Use SDP when:** Production reliability, built-in data quality, automatic dependency management needed
- **Use Notebooks when:** Maximum flexibility, prototyping, custom logic, learning
- **Hybrid when:** SDP for core pipeline (Bronze→Silver), notebooks for exploratory Gold layer
- **Trade-offs:** SDP = less control but more reliability; Notebooks = more control but more maintenance

This demonstrates **scenario-based thinking** - the core skill tested in Professional certification! 🎯
