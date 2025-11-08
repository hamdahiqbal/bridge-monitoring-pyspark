
# Bridge Monitoring — End-to-End Streaming Pipeline in PySpark

This repository implements an end-to-end streaming ETL pipeline for monitoring bridges using simulated IoT sensor data.  
The pipeline follows the Bronze → Silver → Gold medallion architecture and is built with PySpark Structured Streaming.

It mirrors the Databricks Delta Live Tables bridge-monitoring concept but uses pure PySpark and file-based sources (JSON → Parquet) for full local execution.

---

## 1. Project Overview

Three simulated sensor streams are generated for each bridge:

- Temperature  
- Vibration  
- Tilt  

Events flow through the following stages:

1. **Streams (Landing Zone)** – Raw JSON files written continuously by `data_generator.py`.
2. **Bronze Layer** – Immutable Parquet storage with minimal validation and a bronze quarantine zone.
3. **Silver Layer** – Enriched with static bridge metadata, applying strict data-quality checks. Invalid rows are routed to a silver rejected sink.
4. **Gold Layer** – Performs 1-minute windowed and watermarked aggregations across all three sensors to produce the final `gold/bridge_metrics` dataset.

The demonstration notebook (`notebooks/demo.ipynb`) includes:
- Inspection of Bronze, Silver, and Gold tables  
- Data-quality metrics and rejected records  
- Windowed aggregations and watermark behavior  
- Example analytics (e.g., top bridges by vibration)  
- Visualization of the complete data flow DAG (`notebooks/dag.png`)

---

## 2. Repository Structure
````
bridge-monitoring-pyspark/
├─ data_generator/          # JSON event generator
│  └─ data_generator.py
├─ metadata/                # static bridge metadata
│  └─ bridges.csv
├─ notebooks/               # validation and demo
│  ├─ demo.ipynb
│  └─ dag.png
├─ pipelines/               # PySpark ETL scripts
│  ├─ bronze_ingest.py
│  ├─ silver_enrichment.py
│  └─ gold_aggregation.py
├─ scripts/                 # helper scripts
│  ├─ run_all.ps1
│  └─ run_all.sh
├─ .gitignore
└─ README.md
````

The following runtime folders are excluded from version control but are generated automatically during execution:

* `.venv/` — local virtual environment
* `streams/` — raw JSON landing zone
* `bronze/` — ingested immutable Parquet files
* `silver/` — enriched data and rejected records
* `gold/` — final aggregated outputs
* `checkpoints/` — Structured Streaming checkpoints

---

## 3. Environment and Dependencies

### Tested Versions

* Python 3.13.5
* PySpark 3.x (Structured Streaming)

### Virtual Environment Setup

```bash
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

pip install pyspark jupyter
```

If running on Windows, ensure `HADOOP_HOME` and `winutils.exe` are configured for Spark.

---

## 4. Data Model

### Raw Events (in `streams/`)

Each simulated event includes:

* `event_time` — UTC timestamp with random lag (0–60 seconds)
* `bridge_id` — numeric bridge identifier
* `sensor_type` — one of `"temperature"`, `"vibration"`, or `"tilt"`
* `value` — sensor reading (float)
* `ingest_time` — server write timestamp in UTC

Files are written partitioned by date, for example:

```
streams/bridge_temperature/date=YYYY-MM-DD/events_*.json
```

### Bronze Schema

Bronze tables store:

* `event_time` (string)
* `bridge_id` (int)
* `sensor_type` (string)
* `value` (double)
* `ingest_time` (string)
* `event_time_ts` (timestamp)
* `ingest_time_ts` (timestamp)
* `partition_date` (date)

### Silver and Gold

The Silver layer adds static metadata fields (name, location, installation_date).
The Gold layer computes 1-minute windowed metrics with a 2-minute watermark:

* `avg_temperature`
* `max_vibration`
* `max_tilt_angle`

---

## 5. Running the Pipeline

Each stage runs independently as a Structured Streaming process.

### Step 1 – Start the Data Generator

```powershell
cd path\to\bridge-monitoring-pyspark
.\.venv\Scripts\Activate.ps1
python .\data_generator\data_generator.py --duration-seconds 60 --rate 10
```

Examples:

* Regular run: `--duration-seconds 60 --rate 10`
* Deterministic test mode: `--duration-seconds 10 --rate 5 --test-seed 42`

### Step 2 – Bronze Ingestion

```powershell
python .\pipelines\bronze_ingest.py
```

Reads JSON streams, parses timestamps, writes Parquet to `bronze/`, and invalid records to `bronze/rejected`.

### Step 3 – Silver Enrichment

```powershell
python .\pipelines\silver_enrichment.py
```

Reads Bronze data, joins with metadata (`metadata/bridges.csv`), applies data-quality filters, and writes valid and rejected outputs.

### Step 4 – Gold Aggregation

```powershell
python .\pipelines\gold_aggregation.py
```

Performs:

* Watermarking (`2 minutes`)
* 1-minute tumbling window aggregation
* Joins temperature, vibration, and tilt metrics on `(bridge_id, window)`
* Writes results to `gold/bridge_metrics`

All jobs use `checkpoints/` for exactly-once and restart-safe semantics.

### Step 5 – Helper Scripts

For convenience:

* `scripts/run_all.ps1` – Start all pipelines on Windows
* `scripts/run_all.sh` – Start all pipelines on Linux/macOS

---

## 6. Data Quality Rules

### Bronze Layer

* Validates basic JSON structure
* Records missing required fields are written to `bronze/rejected`

### Silver Layer

Enforces strict expectations:

* `event_time_ts` is not null
* `value` is not null
* Valid range per sensor:

  * Temperature: −40 ≤ value ≤ 80
  * Vibration: value ≥ 0
  * Tilt: 0 ≤ value ≤ 90

Invalid records are written to `silver/rejected`.

Example:

```python
from pyspark.sql.functions import count
rejected_df = spark.read.parquet("../silver/rejected")
rejected_df.groupBy("sensor_type").agg(count("*").alias("rejected_count")).show()
```

### Join Success Rate

```python
from pyspark.sql.functions import col
silver_temp = spark.read.parquet("../silver/bridge_temperature")
total = silver_temp.count()
matched = silver_temp.filter(col("name").isNotNull()).count()
print(f"Join success rate: {matched / total * 100:.2f}%")
```

---

## 7. Validation and Analytics Notebook

Launch Jupyter Notebook:

```bash
jupyter notebook
```

Open `notebooks/demo.ipynb`, which includes:

1. Environment verification (`spark.version`)
2. Bronze inspection and event count per minute
3. Silver inspection and rejected record counts
4. Gold inspection (aggregated metrics)
5. Example analytics: Top bridges by vibration
6. Watermark comparison experiment
7. Pipeline DAG visualization (`dag.png`)

Example query:

```python
from pyspark.sql.functions import max as spark_max, col
top_vibration = (
    gold_df.groupBy("bridge_id")
    .agg(spark_max("max_vibration").alias("peak_vibration"))
    .orderBy(col("peak_vibration").desc())
)
top_vibration.show(10, truncate=False)
```

---

## 8. Reproducibility and Testing

Features that ensure consistent and reliable runs:

* Deterministic data generation (`--test-seed`)
* Unique checkpoint locations for each stream (idempotent writes)
* Configurable watermark window for latency experiments

---

## 9. Example End-to-End Run (Windows / PowerShell)

```powershell
cd "C:\path\to\bridge-monitoring-pyspark"
.\.venv\Scripts\Activate.ps1

python .\data_generator\data_generator.py --duration-seconds 60 --rate 10
python .\pipelines\bronze_ingest.py
python .\pipelines\silver_enrichment.py
python .\pipelines\gold_aggregation.py
jupyter notebook .\notebooks\demo.ipynb
```

Equivalent commands for Linux/macOS:

```bash
source .venv/bin/activate
```

---

## 10. Notes for Reviewers and Instructors

* The project uses file-based data sources (JSON → Parquet) for simplicity and full reproducibility.
* Each layer is append-only and uses independent checkpoint directories for exactly-once semantics.
* Bronze and Silver layers include rejected sinks for visible data-quality validation.
* The `demo.ipynb` notebook and `dag.png` diagram together document the complete ETL flow.

To reproduce the exact results used for screenshots:

```powershell
python .\data_generator\data_generator.py --duration-seconds 10 --rate 5 --test-seed 42
```

---

## 11. Final Status

All components have been implemented, tested, and validated:

* Data Generator
* Bronze, Silver, and Gold Streaming Pipelines
* Data-Quality Validation and Rejected Handling
* Structured Streaming Checkpointing and Idempotency
* Jupyter Notebook for Verification and Analytics

This repository represents a complete implementation of the end-to-end PySpark streaming pipeline required for the Bridge Monitoring assignment.


