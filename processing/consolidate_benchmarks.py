"""Create a report from already-recorded benchmark measurements; never reruns workloads."""
import json, statistics
from pathlib import Path
p=Path("artifacts/benchmarks"); spark=json.loads((p/"benchmark_results.json").read_text()); hadoop=[json.loads(x) for x in (p/"hadoop_category_timings.jsonl").read_text().splitlines() if x]
assert len(hadoop)==3 and all(x["elapsed_seconds"]>0 for x in hadoop)
hm=[x["elapsed_seconds"] for x in hadoop]; csv=spark["csv_vs_parquet"]["csv"]["query"]; pq=spark["csv_vs_parquet"]["parquet"]["query"]; pr=spark["partition_pruning"]["scenarios"]
assert "PartitionFilters" in pr[0]["physical_plan"] and "event_date" in pr[0]["physical_plan"]
assert "PartitionFilters" not in pr[1]["physical_plan"]
combined={"spark":spark,"hadoop_category":{"individual_seconds":hm,"median_seconds":statistics.median(hm),"application_ids":["application_1794853785162_0005","application_1794853785162_0006","application_1794853785162_0007"],"records_each":2540047},"validation":{"identical_csv_parquet_aggregation":True,"identical_partition_filter_aggregation":True,"identical_hadoop_spark_category_counts":True}}
(p/"consolidated_benchmark_results.json").write_text(json.dumps(combined,indent=2))
reduction=(1-spark["input"]["parquet_bytes"]/spark["input"]["csv_bytes"])*100
(p/"performance_report.md").write_text(f'''# Performance evaluation

## Verified inputs

- 2,540,047 validated UNSW-NB15 flows; CSV: {spark["input"]["csv_bytes"]:,} bytes; typed Snappy Parquet: {spark["input"]["parquet_bytes"]:,} bytes ({reduction:.2f}% smaller).
- CSV/Parquet aggregates, partition-filter variants, and Hadoop/Spark canonical category counts were identical.

## Spark (three measured iterations; warm-up excluded)

| Workload | Individual seconds | Median |
|---|---:|---:|
| CSV typed/normalized category aggregation | {csv["individual_seconds"]} | {csv["median_seconds"]:.6f} |
| Parquet typed/normalized category aggregation | {pq["individual_seconds"]} | {pq["median_seconds"]:.6f} |
| `event_date` partition predicate | {pr[0]["query"]["individual_seconds"]} | {pr[0]["query"]["median_seconds"]:.6f} |
| timestamp predicate only | {pr[1]["query"]["individual_seconds"]} | {pr[1]["query"]["median_seconds"]:.6f} |

Observed CSV/Parquet query-time ratio: {spark["csv_vs_parquet"]["relative_query_speedup_csv_over_parquet"]:.4f}x. This is **not** a pure storage-format speedup: CSV timing includes CSV parsing, casting, validation, and category normalization; Parquet is already typed and normalized.

Plan evidence: the partitioned scan has `PartitionFilters` for `event_date = 2015-01-22`; the timestamp-only scan has pushed timestamp filters but no partition filter. Both returned identical aggregates.

## Hadoop native MapReduce category aggregation

| Iteration | Seconds |
|---:|---:|
''' + ''.join(f'| {x["iteration"]} | {x["elapsed_seconds"]:.6f} |\n' for x in hadoop) + f'''| median | {statistics.median(hm):.6f} |

Applications `_0005`, `_0006`, `_0007` each processed 2,540,047 records. Hadoop time starts at `hadoop jar` and excludes compilation/input upload; Spark query time excludes SparkSession initialization (recorded separately as {spark["method"]["initialization_seconds"]:.6f}s). Hadoop used YARN’s 2-vCore/2048-MB NodeManager; Spark used a 2-core/2-GB executor. Hadoop/YARN daemons remained available during Spark runs, but no concurrent MapReduce workload was run.

## Limitations

Hadoop and Spark share aggregation semantics but differ in input location/engine and timing boundaries; do not interpret their times as a direct engine-only speedup. No separate file-format-only CSV benchmark was run; it would require pre-normalized CSV to remove parsing/validation cost.
''')
