import json
from pathlib import Path
root=Path("artifacts/benchmarks")
data=json.loads((root/"consolidated_benchmark_results.json").read_text())
assert data["validation"] == {"identical_csv_parquet_aggregation":True,"identical_partition_filter_aggregation":True,"identical_hadoop_spark_category_counts":True}
assert len(data["hadoop_category"]["individual_seconds"]) == 3
assert "PartitionFilters" in data["spark"]["partition_pruning"]["scenarios"][0]["physical_plan"]
assert "PartitionFilters" not in data["spark"]["partition_pruning"]["scenarios"][1]["physical_plan"]
assert (root/"performance_report.md").is_file()
