"""Phase 3 Spark SQL analytics; results are bounded and evidence-based."""
import argparse, csv, json, time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession

QUERY_FILES=["01_traffic_distribution.sql","02_attack_breakdown.sql","03_endpoint_concentration.sql","04_time_trends.sql","05_connection_bursts.sql","06_flow_statistics.sql","07_repeated_communications.sql","08_pattern_rules.sql"]
DEFINITIONS={"01_traffic_distribution":"Observed labelled-flow distribution","02_attack_breakdown":"Observed attacks joined to UNSW's published category taxonomy","03_endpoint_concentration":"Source concentration; observed attacks are label=1","04_time_trends":"Observed labelled flows by ETL-derived event date","05_connection_bursts":"Five-minute per-source window count; evidence only, not an attack label","06_flow_statistics":"Flow, packet/byte, duration and load statistics","07_repeated_communications":"Repeated endpoint communication evidence","08_pattern_rules":"Inferred suspicious normal-flow rules; distinct from observed attacks"}
def main():
 p=argparse.ArgumentParser(); p.add_argument("--input",default="/workspace/data/processed/unsw_nb15"); p.add_argument("--output",default="/workspace/artifacts/analytics"); p.add_argument("--sql-dir",default="/workspace/sql"); p.add_argument("--reference",default="/workspace/reference/unsw_attack_categories.csv"); p.add_argument("--top-n",type=int,default=200); a=p.parse_args()
 if not 1<=a.top_n<=1000: raise SystemExit("--top-n must be 1..1000")
 out=Path(a.output); out.mkdir(parents=True,exist_ok=True); spark=SparkSession.builder.appName("unsw-nb15-sql-analytics").getOrCreate()
 try:
  flows=spark.read.parquet(a.input).select("srcip","dstip","proto","service","stime","ltime","event_date","label","attack_cat_canonical","sbytes","dbytes","Spkts","Dpkts","dur","Sload","Dload")
  flows.createOrReplaceTempView("flows")
  with Path(a.reference).open(encoding="utf-8",newline="") as reference_file: reference_rows=list(csv.DictReader(reference_file))
  spark.createDataFrame(reference_rows).createOrReplaceTempView("attack_category_reference"); source_count=flows.count(); results=[]
  for filename in QUERY_FILES:
   name=Path(filename).stem; sql=(Path(a.sql_dir)/filename).read_text(encoding="utf-8").replace("{{TOP_N}}",str(a.top_n)); started=time.perf_counter(); frame=spark.sql(sql); count=frame.count(); frame.write.mode("overwrite").option("compression","snappy").parquet(str(out/name)); results.append({"name":name,"definition":DEFINITIONS[name],"result_count":count,"elapsed_seconds":round(time.perf_counter()-started,3),"sql_file":filename})
  (out/"analytics_manifest.json").write_text(json.dumps({"created_at_utc":datetime.now(timezone.utc).isoformat(),"source_rows":source_count,"top_n":a.top_n,"queries":results,"rules":{"observed_attack":"label=1 with canonical UNSW category","inferred_rules":"p99 normal-flow bytes/sec and >=100 normal flows/source; evidence only"}},indent=2),encoding="utf-8")
  print(f"ANALYTICS PASSED: source_rows={source_count:,}, queries={len(results)}")
 finally: spark.stop()
if __name__=="__main__": main()
