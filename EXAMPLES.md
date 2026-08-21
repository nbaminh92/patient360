# Educational Examples Guide

This guide walks you through the **step-by-step examples** demonstrating Bronze → Silver → Gold medallion architecture using traditional notebooks and DataFrames.

---

## 🎯 Learning Objectives

By working through these examples, you'll understand:

1. **Bronze Layer**: Raw data ingestion with Auto Loader
2. **Silver Layer**: Data cleaning, deduplication, and incremental processing
3. **Gold Layer**: Business aggregations and analytics-ready datasets
4. **Professional Exam Skills**: Layer responsibilities, transformation patterns, optimization strategies

---

## 📚 Execution Order

### Step 1: Bronze Layer - Raw Ingestion

#### 1a. Patient Data Ingestion
**File:** `examples/bronze/01_Bronze_Patient_Ingestion.ipynb`

**What It Demonstrates:**
- Auto Loader configuration for CSV files
- Schema inference and evolution
- Incremental file processing
- Checkpoint management

**Key Concepts:**
- `cloudFiles.format` = "csv"
- `cloudFiles.schemaLocation` for schema tracking
- `.trigger(availableNow=True)` for incremental batch

**Professional Exam Focus:**
- "How does Auto Loader handle new files?"
- "What happens when the schema changes?"
- "Why use Auto Loader vs. spark.read.csv?"

**Run Time:** ~2 minutes

---

#### 1b. Observation Data Ingestion
**File:** `examples/bronze/01_Bronze_Observation_Ingestion.py`

**What It Demonstrates:**
- Batch CSV ingestion at scale (114K+ records)
- Schema validation
- Source file metadata tracking

**Key Concepts:**
- Manual schema definition
- `_src_file_path` for lineage
- Performance considerations for large files

**Professional Exam Focus:**
- "When to use Auto Loader vs. batch reads?"
- "How to track data lineage?"
- "Cost optimization for large files"

**Run Time:** ~3 minutes

---

### Step 2: Silver Layer - Data Cleaning & Transformation

#### 2a. Patient Auto Loader (Incremental)
**File:** `examples/silver/02_Patient_AutoLoader.ipynb`

**What It Demonstrates:**
- Incremental processing from Bronze to Silver
- Auto Loader for Bronze → Silver transformation
- Handling updates and changes

**Key Concepts:**
- Reading from Delta table with Auto Loader semantics
- Incremental transformation patterns
- Checkpoint-based processing

**Professional Exam Focus:**
- "How to process only new records?"
- "Incremental vs. full refresh trade-offs"
- "When is incremental processing appropriate?"

**Run Time:** ~2 minutes

---

#### 2b. Patient Deduplication
**File:** `examples/silver/03_Silver_Patient_Transform.py`

**What It Demonstrates:**
- Window function deduplication strategy
- Keeping most recent record per patient
- Data quality transformations

**Key Concepts:**
```python
ROW_NUMBER() OVER (
    PARTITION BY Id 
    ORDER BY _src_file_ts DESC
) = 1
```

**Professional Exam Focus:**
- "How to identify duplicates across the entire dataset?"
- "Why window functions require batch processing?"
- "Alternative deduplication strategies?"

**Run Time:** ~2 minutes

---

#### 2c. Observation Data Cleaning
**File:** `examples/silver/03_Silver_Observation_Transform.py`

**What It Demonstrates:**
- Data type conversions
- Date validation and standardization
- Code normalization
- Append-only transformation pattern

**Key Concepts:**
- Column-level transformations
- Data quality checks
- Streaming-friendly logic (no full-table operations)

**Professional Exam Focus:**
- "What transformations are streaming-safe?"
- "How to validate data without blocking ingestion?"
- "Data quality vs. availability trade-offs"

**Run Time:** ~3 minutes

---

### Step 3: Gold Layer - Business Aggregations

#### 3a. Comprehensive Patient Metrics
**File:** `examples/gold/04_Gold_Comprehensive_Metrics.py`

**What It Demonstrates:**
- Patient 360° view (joining multiple entities)
- Pre-aggregated business metrics
- Analytics-ready dataset design

**Key Concepts:**
- Multi-table joins (patients + observations + organizations)
- Aggregation patterns (`COUNT`, `MAX`, `MIN`)
- Denormalized design for BI tools

**Professional Exam Focus:**
- "How to design Gold layer for analytics workloads?"
- "Join strategies for large datasets"
- "When to pre-aggregate vs. on-demand calculations?"

**Run Time:** ~3 minutes

---

## 🔄 Complete Workflow

### End-to-End Execution (30-40 minutes)

```bash
# 1. Bronze Layer - Ingest raw data
Run: examples/bronze/01_Bronze_Patient_Ingestion.ipynb
Run: examples/bronze/01_Bronze_Observation_Ingestion.py

# 2. Silver Layer - Clean and transform
Run: examples/silver/02_Patient_AutoLoader.ipynb
Run: examples/silver/03_Silver_Patient_Transform.py
Run: examples/silver/03_Silver_Observation_Transform.py

# 3. Gold Layer - Business aggregations
Run: examples/gold/04_Gold_Comprehensive_Metrics.py

# 4. Validation queries
# See "Sample Queries" section below
```

---

## 🔍 Sample Queries After Each Layer

### After Bronze Layer

```sql
-- Check patient records ingested
SELECT COUNT(*) as total_patients 
FROM bronze_patients;

-- Check for duplicates
SELECT Id, COUNT(*) as duplicate_count
FROM bronze_patients
GROUP BY Id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Verify observation volume
SELECT COUNT(*) as total_observations 
FROM bronze_observations;

-- Check data freshness
SELECT 
  MIN(_src_file_ts) as first_record,
  MAX(_src_file_ts) as last_record
FROM bronze_patients;
```

### After Silver Layer

```sql
-- Verify deduplication worked
SELECT 
  (SELECT COUNT(*) FROM bronze_patients) as bronze_count,
  (SELECT COUNT(*) FROM silver_patients) as silver_count,
  (SELECT COUNT(*) FROM bronze_patients) - (SELECT COUNT(*) FROM silver_patients) as duplicates_removed;

-- Check for remaining duplicates (should be 0)
SELECT Id, COUNT(*) as dup_count
FROM silver_patients
GROUP BY Id
HAVING COUNT(*) > 1;

-- Validate observation transformations
SELECT 
  COUNT(*) as total_observations,
  COUNT(DISTINCT PATIENT) as unique_patients,
  MIN(DATE) as earliest_observation,
  MAX(DATE) as latest_observation
FROM silver_observations;
```

### After Gold Layer

```sql
-- Patient 360° metrics
SELECT *
FROM gold_patient360_metrics
ORDER BY total_observations DESC
LIMIT 10;

-- Patients with most observations
SELECT 
  patient_id,
  patient_name,
  total_observations,
  first_observation_date,
  most_recent_observation_date
FROM gold_patient360_metrics
WHERE total_observations > 100
ORDER BY total_observations DESC;

-- Healthcare organization analysis
SELECT 
  organization_name,
  COUNT(DISTINCT patient_id) as patient_count,
  SUM(total_observations) as total_obs
FROM gold_patient360_metrics
WHERE organization_name IS NOT NULL
GROUP BY organization_name
ORDER BY patient_count DESC;
```

---

## 🎓 Professional Exam Scenarios

### Scenario: Layer Responsibility

**Question:** *"Where should you perform data deduplication - Bronze, Silver, or Gold layer?"*

**Answer Using Examples:**
- **Bronze:** No deduplication - preserve raw data exactly as received
- **Silver:** ✅ Patient deduplication here (`03_Silver_Patient_Transform.py`)
- **Rationale:** 
  - Bronze = source of truth (immutable)
  - Silver = clean, deduplicated, business-ready
  - Gold = aggregations only (assumes Silver is already clean)

### Scenario: Incremental vs. Batch

**Question:** *"When should you use incremental processing vs. full refresh?"*

**Answer Using Examples:**
- **Incremental:** Observations (`02_Patient_AutoLoader.ipynb`) - append-only data, high volume
- **Batch:** Patient deduplication (`03_Silver_Patient_Transform.py`) - need full dataset visibility
- **Trade-off:** Incremental = lower cost but limited operations; Batch = higher cost but complete flexibility

### Scenario: Auto Loader vs. Manual Read

**Question:** *"When would you use Auto Loader instead of spark.read.csv()?"*

**Answer Using Examples:**
- **Auto Loader:** Bronze ingestion (`01_Bronze_Patient_Ingestion.ipynb`)
  - Incremental file discovery
  - Schema evolution handling
  - Automatic retry and checkpointing
- **Manual Read:** One-time historical load, known file list, simple schemas
- **Trade-off:** Auto Loader = more setup but production-grade; Manual = simpler but limited scalability

---

## 💡 Common Mistakes & Best Practices

### ❌ Common Mistakes

1. **Deduplicating in Bronze Layer**
   - Violates raw data preservation principle
   - Makes it impossible to replay with different dedup logic

2. **Full Refresh in Silver for Append-Only Data**
   - Wastes compute scanning unchanged data
   - Higher costs for no benefit

3. **Joining in Bronze Layer**
   - Bronze should be single-source, no cross-table dependencies
   - Joins belong in Silver (cleaning) or Gold (analytics)

4. **Aggregating in Silver Layer**
   - Silver = row-level transformations only
   - Aggregations belong in Gold

### ✅ Best Practices

1. **Bronze Layer**
   - Immutable raw data
   - Add metadata (_src_file_path, _src_file_ts)
   - No transformations except schema evolution

2. **Silver Layer**
   - Row-level transformations only
   - Deduplication, cleaning, validation
   - Incremental processing where possible

3. **Gold Layer**
   - Business aggregations
   - Multi-table joins
   - Denormalized for analytics
   - Pre-computed metrics

4. **General**
   - Use Auto Loader for production ingestion
   - Incremental processing for append-only data
   - Batch processing only when full-table visibility needed
   - Document layer responsibilities clearly

---

## 🔄 Comparison with Production Pipeline

| Aspect | Educational Examples | Production Pipeline (pipeline_v1) |
|--------|---------------------|----------------------------------|
| **Code Style** | Imperative (explicit DataFrames) | Declarative (table definitions) |
| **Data Quality** | Manual validation queries | Built-in expectations framework |
| **Error Handling** | Try/catch blocks | Automatic (@fail/@drop) |
| **Monitoring** | Custom notebook queries | Pipeline UI + automated dashboard |
| **Learning Curve** | ✅ Easier to understand | Steeper (new syntax) |
| **Production Ready** | Requires hardening | ✅ Production-grade out of box |
| **Flexibility** | ✅ Maximum control | Constrained by framework |

**When to Use Each:**
- **Examples:** Learning, prototyping, maximum flexibility needed
- **Production Pipeline:** Production workloads, strict data quality, automatic monitoring

---

## 📊 Expected Results

After running all examples:

### Bronze Layer
- **bronze_patients**: 339 records
- **bronze_observations**: 114,342 records

### Silver Layer
- **silver_patients**: 169 records (50.15% deduplication)
- **silver_observations**: 114,342 records (cleaned)

### Gold Layer
- **gold_patient360_metrics**: 169 records with aggregated metrics per patient

---

## 🎯 Next Steps

1. **Run all examples** in order (Bronze → Silver → Gold)
2. **Compare with production pipeline** (`pipeline_v1/`)
3. **Try variations:**
   - Different deduplication logic
   - Additional data quality checks
   - Alternative aggregation metrics
4. **Practice explaining trade-offs** for exam scenarios

---

## 📚 Additional Resources

- [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)
- [Auto Loader Best Practices](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Professional Exam Study Guide](https://www.databricks.com/learn/certification/data-engineer-professional)

---

**Ready to dive in?** Start with `examples/bronze/01_Bronze_Patient_Ingestion.ipynb`! 🚀
