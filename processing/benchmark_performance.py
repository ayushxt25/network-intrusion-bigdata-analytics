"""Reproducible Phase 4 performance measurements; no analytics outputs are changed."""
import argparse, json, statistics, time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from processing.unsw_nb15_common import EXPECTED_FILES, read_raw, typed_and_classified

DATE="2015-01-22"; ITERATIONS=3
def now(): return datetime.now(timezone.utc).isoformat()
def seconds(f):
    t=time.perf_counter(); value=f(); return time.perf_counter()-t,value
def rows(frame): return [(r["label"],r["attack_cat_canonical"],r["count"]) for r in frame.orderBy("label","attack_cat_canonical").collect()]
def aggregate(frame): return frame.groupBy("label","attack_cat_canonical").count()
def plan(frame): return frame._sc._jvm.PythonSQLUtils.explainString(frame._jdf.queryExecution(), "formatted")
def stat(samples): return {"individual_seconds":[round(x,6) for x in samples],"median_seconds":round(statistics.median(samples),6)}
def size(path): return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file())
def run_case(name, loader, expected):
    warm=loader(); assert rows(aggregate(warm))==expected, name+" warm-up mismatch"
    results=[]
    for _ in range(ITERATIONS):
        load_s, frame=seconds(loader); scan_s,count=seconds(frame.count); query_s,result=seconds(lambda:rows(aggregate(frame)))
        assert count==2540047 and result==expected, name+" correctness mismatch"
        results.append({"load_metadata_seconds":round(load_s,6),"load_full_scan_seconds":round(scan_s,6),"query_seconds":round(query_s,6)})
    return {"name":name,"iterations":results,"query":stat([x["query_seconds"] for x in results])}
def main():
    p=argparse.ArgumentParser(); p.add_argument("--raw",default="/workspace/data/raw/UNSW-NB15"); p.add_argument("--parquet",default="/workspace/data/processed/unsw_nb15"); p.add_argument("--output",default="/workspace/artifacts/benchmarks"); p.add_argument("--hadoop-category",default="/workspace/artifacts/hadoop/category/part-r-00000"); a=p.parse_args(); out=Path(a.output); out.mkdir(parents=True,exist_ok=True)
    init_s,spark=seconds(lambda:SparkSession.builder.appName("unsw-performance-benchmark").config("spark.sql.shuffle.partitions","16").getOrCreate())
    try:
        raw_paths=[str(Path(a.raw)/x) for x in EXPECTED_FILES]
        raw=lambda:typed_and_classified(read_raw(spark,raw_paths)).filter("rejection_reason = ''").select("label","attack_cat_canonical","stime","event_date")
        pq=lambda:spark.read.parquet(a.parquet).select("label","attack_cat_canonical","stime","event_date")
        expected=rows(aggregate(pq()))
        hadoop={line.split("\t")[0]:int(line.split("\t")[1]) for line in Path(a.hadoop_category).read_text().splitlines() if line}
        spark_categories={r["attack_cat_canonical"]:r["count"] for r in pq().groupBy("attack_cat_canonical").count().collect()}
        assert hadoop==spark_categories, "Hadoop/Spark category aggregation mismatch"
        csv=run_case("csv_typed",raw,expected); parquet=run_case("parquet_snappy",pq,expected)
        # Same date/result; only the pruned case predicates the partition column.
        pruned=pq().filter(F.col("event_date")==F.lit(DATE)); unpruned=pq().filter(F.to_date("stime")==F.lit(DATE))
        expected_day=rows(aggregate(pruned)); assert rows(aggregate(unpruned))==expected_day
        prune=[]
        for label,frame in (("partition_pruned",pruned),("timestamp_filter_no_partition_predicate",unpruned)):
            warm=rows(aggregate(frame)); assert warm==expected_day
            values=[]
            for _ in range(ITERATIONS):
                elapsed,result=seconds(lambda:rows(aggregate(frame))); assert result==expected_day; values.append(elapsed)
            prune.append({"name":label,"query":stat(values),"physical_plan":plan(aggregate(frame))})
        assert "PartitionFilters" in prune[0]["physical_plan"] and "event_date" in prune[0]["physical_plan"], "partition filter absent from plan"
        result={"created_at_utc":now(),"method":{"warmups_excluded":True,"iterations":ITERATIONS,"initialization_seconds":round(init_s,6),"load_definition":"DataFrame construction plus a separate full count scan","query":"label/category count aggregation; canonical normalization"},"input":{"records":2540047,"csv_bytes":size(a.raw),"parquet_bytes":size(a.parquet)},"csv_vs_parquet":{"csv":csv,"parquet":parquet,"relative_query_speedup_csv_over_parquet":round(csv["query"]["median_seconds"]/parquet["query"]["median_seconds"],4)},"partition_pruning":{"date":DATE,"scenarios":prune},"hadoop_vs_spark":{"identical_category_counts":True,"semantics":"canonical category counts including Normal","hadoop_timing":"not measured by this Spark runner; use hadoop/benchmark_category.sh","hadoop_category_output":"/data/unsw/out/category/part-r-00000"}}
        (out/"benchmark_results.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
        (out/"performance_summary.md").write_text("# Benchmark summary\n\nResults are in `benchmark_results.json`; timings exclude one warm-up and use three measured iterations. Hadoop timing is intentionally absent until its dedicated runner completes.\n",encoding="utf-8")
        print(json.dumps(result["csv_vs_parquet"],indent=2))
    finally: spark.stop()
if __name__=="__main__": main()
