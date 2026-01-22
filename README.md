# spark-delta-semiconductor-lakehouse

# End-to-End Spark + Delta Lakehouse for Semiconductor Manufacturing Analytics

This project implements a **production-style Spark + Delta Lakehouse** pipeline to process high-volume semiconductor fabrication telemetry data. It demonstrates **bronze / silver / gold modeling**, **data quality enforcement**, **incremental ingestion**, and **analytics-ready aggregations** using PySpark and Delta Lake.

The design mirrors real-world data platforms used in **manufacturing and semiconductor environments**.

---

## Architecture Overview

Raw CSV Sensor Data  
→ **Bronze (Delta)** – raw ingest, incremental file tracking  
→ **Silver (Delta)** – cleaning, deduplication, partitioning, DQ  
→ **Gold (Delta)** – daily yield and defect analytics  

---

## Dataset

Synthetic but realistic **semiconductor fabrication telemetry** representing:

- Manufacturing tools (ETCH, CVD, IMPLANT)
- Wafer-level process steps
- Sensor measurements
- Defect counts and yield indicators

### Scale
- 150,000+ raw events
- 5 tools across 8 days
- Partitioned by event date

---

## Tech Stack

- PySpark 3.5
- Delta Lake 3.2
- Spark SQL
- Local Spark (Mac, `local[*]`)
- YAML-based configuration

---

## Key Features

- Bronze / Silver / Gold Delta Lakehouse modeling
- Incremental file-based ingestion
- Deterministic deduplication using hash keys
- Fail-fast data quality checks:
  - Null checks
  - Value constraints
  - Uniqueness
  - Freshness
- Partitioning by `event_date`
- Broadcast joins for small dimension tables
- Correctness validation via reconciliation tests
- Delta Lake audit history and ACID guarantees

---

## Project Structure

spark-delta-semiconductor-lakehouse/
│
├── data/
│ └── raw/ # Generated CSV telemetry files
│
├── lakehouse/
│ ├── bronze/
│ ├── silver/
│ ├── gold/
│ └── checkpoints/
│
├── src/
│ ├── ingest_bronze.py
│ ├── transform_silver.py
│ ├── dq_checks.py
│ ├── build_gold.py
│ └── utils.py
│
├── configs/
│ └── config.yaml
│
├── screenshots/
│ ├── delta_history_silver.png
│ ├── silver_gold_reconciliation.png
│ ├── gold_tool_metrics.png
│ └── silver_partitions.png
│
├── generate_semiconductor_data.py
├── requirements.txt
└── README.md


---

## How to Run

### 1. Install dependencies
```bash
pip install pyspark==3.5.1 delta-spark==3.2.0 pyyaml
```
### 2. Generate synthetic data
```bash
python generate_semiconductor_data.py
```
### 3. Run the pipeline
```bash
python -m src.ingest_bronze
python -m src.transform_silver
python -m src.dq_checks
python -m src.build_gold
```
---

## Data Quality & Validation

### The pipeline enforces multiple correctness checks:

#### Silver rows contain no null timestamps or invalid values
#### Deduplication ensures unique event keys
#### Yield flags constrained to valid values (0/1)
#### Latest partition freshness validation
#### Bronze → Silver → Gold reconciliation

---

## Example Gold Outputs

### Gold tables include:
#### Tool-level daily yield metrics
##### Yield rate
##### Average defects
##### Sensor percentiles
#### Process-step daily analytics

### See /screenshots for:
#### Delta Lake history
#### Partitioned Silver tables
#### Gold metrics output
#### Reconciliation checks

---

## Delta Lake Audit History
### Delta Lake provides full auditability and versioning:
#### Versioned writes
#### Partition-aware operations
#### Engine and operation metadata
### This ensures reliability and traceability of data changes.


---

## Why This Project
### This project demonstrates readiness to work on industrial-scale data platforms by combining:
#### Distributed data processing
#### Data reliability and quality enforcement
#### Analytics enablement
#### Performance-aware Spark engineering
### It reflects real-world patterns used in manufacturing, semiconductor, and AI-driven data systems.

---

## Future Enhancements
#### Delta MERGE for late-arriving updates
#### Streaming ingestion (Spark Structured Streaming)
#### CI/CD validation for data pipelines
#### Data quality dashboards
