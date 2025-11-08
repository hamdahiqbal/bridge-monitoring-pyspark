---

# Bridge Monitoring – End-to-End Streaming Pipeline in PySpark

This repository implements an end-to-end **streaming ETL pipeline** for monitoring bridges using simulated IoT sensors.
The pipeline follows the **Bronze → Silver → Gold medallion architecture** and is built with **PySpark Structured Streaming**.

It closely mirrors the classic Databricks Delta Live Tables bridge-monitoring example, but uses **pure PySpark + file sources** (JSON → Parquet) and can run locally on a laptop.

---

## 1. High-level overview

Three simulated sensor streams are generated for each bridge:

* **Temperature**
* **Vibration**
* **Tilt**

Events flow through the following stages:

1. **Streams (landing zone)** – raw JSON files written continuously by `data_generator.py`.
2. **Bronze layer** – immutable raw Parquet, minimal validation, plus a **bronze quarantine**.
3. **Silver layer** – enriched with static **bridge metadata**, with strict data-quality rules and a **silver rejected** sink.
4. **Gold layer** – windowed, watermarked aggregations joined across all three sensors to produce `gold/bridge_metrics`.

The notebook **`notebooks/demo.ipynb`** demonstrates:

* Inspecting Bronze/Silver/Gold tables
* Data-quality results (rejected rows)
* Windowed metrics
* Example analytics (top bridges by vibration)
* A small watermark experiment
* The overall DAG (from `notebooks/dag.png`)

---

## 2. Repository structure

```text
bridge-monitoring-pyspark/
├─ .venv/                     # local virtual environment (gitignored)
├─ bronze/                    # bronze Parquet outputs (gitignored)
│  ├─ bridge_temperature/
│  ├─ bridge_vibration/
│  ├─ bridge_tilt/
│  └─ rejected/              # bronze quarantine
├─ silver/                    # silver Parquet outputs (gitignored)
│  ├─ bridge_temperature/
│  ├─ bridge_vibration/
│  ├─ bridge_tilt/
│  └─ rejected/              # silver rejected records
├─ gold/
│  └─ bridge_metrics/        # final 1-minute metrics (gitignored)
├─ streams/                  # landing zone for raw JSON events (gitignored)
│  ├─ bridge_temperature/
│  ├─ bridge_vibration/
│  └─ bridge_tilt/
├─ checkpoints/              # streaming checkpoints per query (gitignored)
│  ├─ bronze_temperature/
│  ├─ bronze_vibration/
│  ├─ bronze_tilt/
│  ├─ bronze_rejected/
│  ├─ silver_temperature/
│  ├─ silver_vibration/
│  ├─ silver_tilt/
│  ├─ silver_rejected/
│  └─ gold_bridge_metrics/
├─ data_generator/
│  └─ data_generator.py      # JSON event generator
├─ metadata/
│  └─ bridges.csv            # static bridge metadata
├─ notebooks/
│  ├─ demo.ipynb             # validation & analytics notebook
│  └─ dag.png                # pipeline DAG / architecture diagram
├─ pipelines/
│  ├─ bronze_ingest.py       # Streams → Bronze
│  ├─ silver_enrichment.py   # Bronze → Silver (+ DQ + metadata join)
│  └─ gold_aggregation.py    # Silver → Gold (window + watermark + joins)
├─ scripts/
│  ├─ run_all.sh             # convenience script (bash)
│  └─ run_all.ps1            # convenience script (PowerShell, Windows)
├─ .gitignore
└─ README.md
```

---

## 3. Environment & dependencies

The project is designed to run on a local machine.

### 3.1. Tested versions

* **Python**: 3.13.5
* **PySpark**: 3.x (Structured Streaming)

Any recent 3.x PySpark + matching Spark runtime should work.

### 3.2. Creating the virtual environment

From the project root:

```bash
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

pip install pyspark jupyter
```

If you are on Windows and Spark requires `winutils.exe`, make sure your Hadoop/Spark environment is configured (e.g. `HADOOP_HOME`, etc.) before starting the pipelines.

---

## 4. Data model

### 4.1. Raw event schema (JSON in `streams/`)

Each generated event has:

* `event_time` – ISO-8601 UTC timestamp (with random 0–60s lag)
* `bridge_id` – integer bridge identifier
* `sensor_type` – `"temperature" | "vibration" | "tilt"`
* `value` – measurement value (float)
* `ingest_time` – server write time in ISO-8601 UTC

Events are written to partitioned directories, e.g.:

```text
streams/bridge_temperature/date=YYYY-MM-DD/events_*.json
```

### 4.2. Bronze schema (Parquet)

Bronze tables add parsed and processing columns:

* `event_time` (string)
* `bridge_id` (int)
* `sensor_type` (string)
* `value` (double)
* `ingest_time` (string)
* `event_time_ts` (timestamp)
* `ingest_time_ts` (timestamp)
* `partition_date` (date)

### 4.3. Silver / Gold

Silver inherits bronze columns and adds **bridge metadata** (name, location, installation date).

Gold `bridge_metrics` contains **1-minute windowed** metrics:

* `bridge_id`
* `window_start`, `window_end`
* `avg_temperature`
* `max_vibration`
* `max_tilt_angle`
* plus metadata columns for readability

All aggregations use a **2-minute watermark** on `event_time_ts` to handle late data.

---

## 5. Running the pipeline

You can run each component manually or use the helper scripts.

### 5.1. Start the data generator

In **one terminal** (with virtualenv activated):

```powershell
# Windows / PowerShell
cd path\to\bridge-monitoring-pyspark
.\.venv\Scripts\Activate.ps1

python .\data_generator\data_generator.py --duration-seconds 60 --rate 10
```

Examples:

* Normal run: `--duration-seconds 60 --rate 10`
* Deterministic test run: `--duration-seconds 10 --rate 5 --test-seed 42`

Generated files appear in `streams/bridge_*`.

### 5.2. Run Bronze ingestion

In **a second terminal**:

```powershell
cd path\to\bridge-monitoring-pyspark
.\.venv\Scripts\Activate.ps1

python .\pipelines\bronze_ingest.py
```

This starts three Structured Streaming queries which:

* Read JSON events from `streams/…`
* Parse timestamps
* Write Parquet to `bronze/bridge_*`
* Write malformed records to `bronze/rejected`

Leave this process running while data is generated.

### 5.3. Run Silver enrichment

In **a third terminal**:

```powershell
cd path\to\bridge-monitoring-pyspark
.\.venv\Scripts\Activate.ps1

python .\pipelines\silver_enrichment.py
```

Silver:

* Reads bronze Parquet as streaming source
* Joins with static `metadata/bridges.csv` on `bridge_id`
* Applies data-quality rules (see section 6)
* Writes good rows to `silver/bridge_*`
* Sends failed rows to `silver/rejected`

### 5.4. Run Gold aggregation

In **a fourth terminal**:

```powershell
cd path\to\bridge-monitoring-pyspark
.\.venv\Scripts\Activate.ps1

python .\pipelines\gold_aggregation.py
```

Gold:

* Reads three silver streams
* Applies `withWatermark("event_time_ts", "2 minutes")`
* Calculates 1-minute tumbling window metrics:

  * avg temperature
  * max vibration
  * max tilt angle
* Joins the three aggregated streams on `(bridge_id, window)`
* Writes `gold/bridge_metrics` as Parquet

All streaming jobs use checkpoint directories under `checkpoints/` to provide **exactly-once semantics** and idempotency.

### 5.5. Convenience scripts

For convenience:

* `scripts/run_all.ps1` – example PowerShell script to start the three pipelines on Windows.
* `scripts/run_all.sh` – equivalent shell script for bash-style environments.

You can adapt these to your environment (e.g. open new terminals, or run in the background).

---

## 6. Data-quality checks

Data-quality is enforced in two stages.

### 6.1. Bronze (minimal)

* Ensures JSON can be parsed into the expected schema.
* Records that cannot be parsed or are missing required columns are written to **`bronze/rejected`**.

### 6.2. Silver (strict expectations)

For each sensor type, the pipeline applies expectations on the streaming DataFrame:

* `event_time_ts` **not null**
* `value` **not null**
* Sensor-specific ranges:

  * **Temperature**: `-40°C ≤ value ≤ 80°C`
  * **Vibration**: `value ≥ 0`
  * **Tilt**: `0° ≤ value ≤ 90°`

Valid rows are written to `silver/bridge_*`; invalid rows are written to **`silver/rejected`**.

The notebook includes a cell:

```python
from pyspark.sql.functions import count

rejected_df = spark.read.parquet("../silver/rejected")
rejected_df.groupBy("sensor_type").agg(count("*").alias("rejected_count")).show()
```

to show **rejected rows per sensor**.

### 6.3. Join success rate

Silver also performs a **stream-static join** with `metadata/bridges.csv`.
The notebook demonstrates how to compute the join success rate, e.g.:

```python
from pyspark.sql.functions import col

silver_temp = spark.read.parquet("../silver/bridge_temperature")
total = silver_temp.count()
matched = silver_temp.filter(col("name").isNotNull()).count()

print(f"Join success rate (temperature): {matched / total * 100:.2f}%")
```

---

## 7. Validation & analytics (demo notebook)

Open Jupyter from the project root:

```bash
jupyter notebook
```

Then open **`notebooks/demo.ipynb`**.

The notebook is organised into sections:

1. **Environment check**

   * Creates a SparkSession, prints `spark.version`, and shows paths.

2. **Inspect Bronze layer**

   * Reads Parquet from `../bronze/…`
   * Shows schemas and example rows.
   * Counts events per minute using `window("event_time_ts", "1 minute")`.

3. **Inspect Silver layer**

   * Reads silver tables and `silver/rejected`.
   * Prints schemas and row counts.
   * Shows rejected rows grouped by `sensor_type`.

4. **Inspect Gold layer**

   * Reads `../gold/bridge_metrics`.
   * Displays schema and a sample of the resulting metrics.

5. **Example analytics – top bridges by max vibration**

   ```python
   from pyspark.sql.functions import max as spark_max, col

   top_vibration = (
       gold_df
       .groupBy("bridge_id")
       .agg(spark_max("max_vibration").alias("peak_vibration"))
       .orderBy(col("peak_vibration").desc())
   )

   top_vibration.show(10, truncate=False)
   ```

6. **Watermark behaviour experiment**

   * Compares a large watermark vs a very small watermark to illustrate how late events may be dropped when they fall outside the watermark horizon.

7. **Pipeline DAG / architecture diagram**

   * Displays `dag.png`, a static diagram illustrating the flow:
     **streams → bronze → silver → gold** with metadata and rejected paths.

This notebook serves as the main **validation and demo artifact** for the assignment.

---

## 8. Testing & reproducibility

The project includes several features specifically for testing and reproducibility:

* **Deterministic generator**:
  `data_generator.py` supports `--test-seed` to generate the same sequence of events for repeatable experiments.

* **Idempotent outputs**:
  All streaming writers use dedicated `checkpointLocation`s under `checkpoints/`.
  This ensures that if the job is restarted, it resumes from the last committed offsets.

* **Watermark experiments**:
  The notebook demonstrates how different watermark settings (too small vs sufficiently large) affect late events and aggregated outputs.

---

## 9. Example command summary

For a full local run on Windows / PowerShell:

```powershell
# 0. From repo root
cd "C:\path\to\bridge-monitoring-pyspark"
.\.venv\Scripts\Activate.ps1

# 1. Generate data (60s, ~10 events/sec)
python .\data_generator\data_generator.py --duration-seconds 60 --rate 10

# 2. In a new terminal – Bronze
python .\pipelines\bronze_ingest.py

# 3. In another terminal – Silver
python .\pipelines\silver_enrichment.py

# 4. In another terminal – Gold
python .\pipelines\gold_aggregation.py

# 5. After some data has been processed, open notebook
jupyter notebook .\notebooks\demo.ipynb
```

On Linux/macOS, the commands are analogous, with `source .venv/bin/activate` instead of the PowerShell activation.

---

## 10. Notes for reviewers / instructors

* The project deliberately uses **file-based sources** (JSON → Parquet) instead of Kafka to keep the setup lightweight and reproducible on a single machine.
* All medallion layers are **append-only**, with explicit `checkpointLocation`s for each streaming query.
* Both **bronze** and **silver** layers include quarantine/rejected sinks to make data-quality behaviour observable.
* The notebook plus `dag.png` together act as the **documentation of the DAG and execution flow**, as required in the assignment brief.

If you want to reproduce the exact runs used for screenshots in the report, you can use the deterministic generator command:

```powershell
python .\data_generator\data_generator.py --duration-seconds 10 --rate 5 --test-seed 42
```

