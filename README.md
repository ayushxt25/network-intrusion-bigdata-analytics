# Large-Scale Network Intrusion Analytics and Attack Pattern Discovery

Phase 1 provides a reproducible local Spark foundation: one standalone Spark master, one worker, a one-shot distributed smoke job, and a Streamlit health page. No Hadoop, Hive, Kafka, ML, database, dataset ingestion, analytical dashboard, or benchmarks are included yet.

## Prerequisites

- Windows 10/11 with Docker Desktop running in Linux-container mode.
- Allocate Docker Desktop 8 GB RAM and 8 CPUs when available; this Compose stack caps its services at 7.5 GB and 7 CPUs.
- Git is optional. No host Spark, Hadoop, Hive, Java, or Python packages are required for the Docker workflow.

## Start services (PowerShell)

```powershell
Set-Location C:\Users\ayush\OneDrive\Desktop\network-intrusion-bigdata-analytics
docker compose up -d --build spark-master spark-worker dashboard
docker compose ps
```

If Docker Desktop provides the legacy `docker-compose` command instead, substitute it for `docker compose` in every command below.

Open:

- Spark master UI: http://localhost:8080
- Spark worker UI: http://localhost:8081
- Dashboard health page: http://localhost:8501

## Run the cluster smoke job

Wait until the Spark worker appears in the master UI, then run:

```powershell
docker compose --profile jobs run --rm analytics
```

Expected final message:

```text
SMOKE TEST PASSED: aggregation, Spark SQL, and Snappy Parquet round-trip.
```

The synthetic fixture is written to `artifacts/smoke_parquet/`; it is ignored by Git.

## Run automated assertions

This runs the same assertions locally inside the pinned Spark image (not on the Windows host):

```powershell
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master local[2] /workspace/tests/test_smoke_job.py
```

## Health procedure

```powershell
docker compose ps
Invoke-WebRequest http://localhost:8501/_stcore/health
Invoke-WebRequest http://localhost:8080
docker compose logs --tail=100 dashboard
```

The dashboard reports whether the synthetic Parquet fixture exists. It intentionally does not query a dataset or render charts in Phase 1.

## Stop services

```powershell
docker compose down
```

## Layout

```text
processing/  Spark jobs
sql/         future Spark SQL files
dashboard/   Streamlit application
tests/       automated smoke checks
data/raw/    untracked source datasets
data/processed/ untracked Parquet outputs
artifacts/   untracked generated outputs
```
