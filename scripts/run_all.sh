#!/usr/bin/env bash
# Run full bridge monitoring pipeline (Linux/macOS WSL)

set -e

# Go to project root (file is in scripts/)
cd "$(dirname "$0")/.."

# Activate virtualenv (adjust path if needed)
source .venv/bin/activate

echo "Starting data generator for 60s..."
python data_generator/data_generator.py --duration-seconds 60 --rate 10 &

echo "Starting Bronze ingestion..."
python pipelines/bronze_ingest.py &

echo "Starting Silver enrichment..."
python pipelines/silver_enrichment.py &

echo "Starting Gold aggregation..."
python pipelines/gold_aggregation.py &

echo "All streaming jobs started. Press Ctrl+C to stop."
wait
