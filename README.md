# Large-Scale Network Intrusion Analytics and Attack Pattern Discovery

Reproducible local project for UNSW-NB15 network-flow analytics: Dockerized Spark SQL/Parquet ETL and analytics, native Hadoop MapReduce/HDFS/YARN aggregates, R summaries, benchmarks, and a Streamlit dashboard backed only by precomputed artifacts. It does not use ML, Hive, Kafka, or a database service.

## Prerequisites

- Windows 10/11 with Docker Desktop running in Linux-container mode.
- Allocate Docker Desktop about 12 GB RAM and 8 CPUs when available. Run Spark and Hadoop *jobs* sequentially on a 16 GB host; their persistent services may coexist.
- Git is optional. No host Spark, Hadoop, Hive, Java, or Python packages are required for the Docker workflow.

## Start services (PowerShell)

```powershell
Set-Location C:\path\to\network-intrusion-bigdata-analytics
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
curl.exe -fsS http://localhost:8501/_stcore/health
curl.exe -fsS http://localhost:8080
docker compose logs --tail=100 dashboard
```

The dashboard reads bounded, precomputed Spark analytical artifacts. It does not scan raw CSV, Parquet, HDFS, or run Spark/Hadoop jobs at request time.

## Dashboard demonstration

```powershell
docker compose up -d --build dashboard
curl.exe -fsS http://localhost:8501/_stcore/health
# Open http://localhost:8501. The dashboard reads only artifacts/analytics/.
docker compose exec dashboard python tests/test_dashboard_contract.py
```

## Phase 2A: UNSW-NB15 acquisition and validation

Use the official UNSW source: <https://research.unsw.edu.au/projects/unsw-nb15-dataset>. It documents 2,540,044 flow records in `UNSW-NB15_1.csv` through `UNSW-NB15_4.csv`; these full, headerless files retain `srcip`, `sport`, `dstip`, `dsport`, `Stime`, and `Ltime`. Do not use the smaller 175,341/82,332 train/test subsets for the distributed flow analysis.

The source download is hosted on UNSW SharePoint and may require browser authentication. Manually download the four full CSVs and place them exactly here (the directory is ignored by Git):

```powershell
New-Item -ItemType Directory -Force data\raw\UNSW-NB15
# Place UNSW-NB15_1.csv, UNSW-NB15_2.csv, UNSW-NB15_3.csv, UNSW-NB15_4.csv here.
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master spark://spark-master:7077 /workspace/processing/unsw_nb15_validate.py --input-dir /workspace/data/raw/UNSW-NB15 --report-dir /workspace/artifacts/unsw_nb15_validation
```

Reports are written to ignored `artifacts/unsw_nb15_validation/`. The validator reads raw CSV as strings using the official 49-column order, reports malformed records/types/missingness/duplicates/labels/categories/protocols/services, and makes no repairs or silent row drops.

Usage: academic research is free in perpetuity; commercial use requires author agreement, and the cited UNSW papers are required by the provider. See the official landing page for current terms and provenance.
Detailed provenance, source-size limits, and validation contract: [docs/unsw_nb15_provenance.md](docs/unsw_nb15_provenance.md).

## Phase 2B: canonical Parquet ETL

The ETL keeps raw CSV immutable, retains duplicate records, and writes only valid canonical rows to Snappy Parquet. Invalid labels/categories/timestamps/non-port numeric values are written with reasons to an ignored quarantine dataset. Ports retain `sport_raw`/`dsport_raw`; decimal and `0x` values are parsed when in range, while special values remain null in typed port fields.

```powershell
docker compose up -d spark-master spark-worker
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master spark://spark-master:7077 /workspace/processing/unsw_nb15_etl.py --input-dir /workspace/data/raw/UNSW-NB15 --processed-dir /workspace/data/processed/unsw_nb15 --artifact-dir /workspace/artifacts/unsw_nb15_etl
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master local[2] /workspace/tests/test_unsw_nb15.py
```

Outputs: `data/processed/unsw_nb15/` (partitioned by validated `event_date`), `artifacts/unsw_nb15_etl/quarantine/`, and `artifacts/unsw_nb15_etl/etl_manifest.json`. Event-date partitioning supports time scans without IP cardinality or attack-category skew; writes use 16 shuffle partitions. To recover from an interrupted run, rerun the command: output writes use `overwrite`; raw CSV files are never changed.

Observed source-release note: this downloaded file set has 2,540,047 rows, not the UNSW-published 2,540,044. Physical line counts are 700,001 in each of files 1–3 and 440,044 in file 4; the three surplus rows are valid parsed rows, not headers or malformed records. The ETL preserves them. Its 480,633 duplicate figure is exact full-row duplication across the 49 raw fields; duplicates are reported but never removed. Category rules trim whitespace, map `Backdoors` to `Backdoor`, and map `None`/null-like categories to `Normal` only when `label=0`.

## Phase 3: Spark SQL analytics

The analytics runner registers canonical Parquet as `flows` and the [UNSW category taxonomy](reference/unsw_attack_categories.csv) as a documented reference view. It writes bounded Snappy Parquet results and a manifest; it performs no ML or dashboard update.

```powershell
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master spark://spark-master:7077 /workspace/processing/run_analytics.py --input /workspace/data/processed/unsw_nb15 --output /workspace/artifacts/analytics --top-n 200
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master local[2] /workspace/tests/test_analytics.py
```

SQL files cover label/category distribution; taxonomy join and protocol/service breakdown; endpoint concentration; time trends; five-minute window bursts; flow/byte/packet/load statistics; repeated communications; and evidence-only suspicious rules. Results are under `artifacts/analytics/`; inspect `analytics_manifest.json` for definitions and row counts. `label=1` is an observed dataset label. Rules identify only inferred suspicious behaviour and never relabel a flow as an attack.

## Stop services

```powershell
docker compose down
```

## Hadoop MapReduce, R, and presentation run

```powershell
docker compose --profile hadoop up -d namenode datanode resourcemanager nodemanager
docker compose --profile hadoop ps
docker compose --profile hadoop run --rm hadoop-jobs
docker compose --profile r run --rm r-analysis
docker compose --profile jobs run --rm --entrypoint /opt/spark/bin/spark-submit analytics --master spark://spark-master:7077 /workspace/processing/run_analytics.py
```

HDFS UI: `http://localhost:9870`; YARN UI: `http://localhost:8088`. Hadoop outputs are copied to ignored `artifacts/hadoop/`; R creates graphs and JSON only after those aggregates exist. On 16 GB RAM, run Hadoop services and Spark jobs sequentially. Source-only ZIP:

### Final demonstration (uses existing artifacts; does not rerun processing)

```powershell
Set-Location C:\path\to\network-intrusion-bigdata-analytics
docker compose --profile hadoop up -d namenode datanode resourcemanager nodemanager
docker compose up -d spark-master spark-worker dashboard
docker compose ps
curl.exe -fsS http://localhost:8501/_stcore/health
docker compose exec namenode hdfs dfs -test -e /data/unsw/out/category/_SUCCESS
docker compose exec resourcemanager yarn node -list
docker compose exec dashboard python tests/test_dashboard_contract.py
```

Open `http://localhost:8501` for the dashboard, `http://localhost:9870` for HDFS, `http://localhost:8088` for YARN, and `http://localhost:8080` for Spark. Do not run ETL, analytics, MapReduce, R, or benchmarks during this demonstration unless intentionally regenerating their ignored outputs.

The Hadoop profile first runs a root-only volume initializer, then starts Hadoop as its normal user. The NameNode formats only an empty `hadoop_name` volume. If it reports a nonempty directory without `current/VERSION`, do not delete or reformat the volume: inspect it first with `docker compose --profile hadoop run --rm --no-deps --entrypoint bash hadoop-init -lc "find /data/dfs/name -maxdepth 3 -ls"`.

```powershell
Compress-Archive -Path compose.yaml,.dockerignore,docker,hadoop,hadoop-conf,processing,reference,r,sql,tests,dashboard,requirements,docs,README.md,.gitignore -DestinationPath ..\network-intrusion-bigdata-analytics-source.zip -Force
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
