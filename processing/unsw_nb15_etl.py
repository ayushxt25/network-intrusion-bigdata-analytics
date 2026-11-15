"""Phase 2B: immutable raw CSV to canonical Snappy Parquet plus quarantine."""
import argparse, hashlib, json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from processing.unsw_nb15_common import COLUMNS, EXPECTED_FILES, read_raw, typed_and_classified
def sha(path):
 h=hashlib.sha256()
 with path.open("rb") as f:
  for b in iter(lambda:f.read(1048576),b""): h.update(b)
 return h.hexdigest()
def main():
 p=argparse.ArgumentParser(); p.add_argument("--input-dir",default="/workspace/data/raw/UNSW-NB15"); p.add_argument("--processed-dir",default="/workspace/data/processed/unsw_nb15"); p.add_argument("--artifact-dir",default="/workspace/artifacts/unsw_nb15_etl"); a=p.parse_args(); files=[Path(a.input_dir)/n for n in EXPECTED_FILES]
 if not all(x.is_file() for x in files): raise SystemExit("Missing one or more official UNSW-NB15 CSV files.")
 Path(a.artifact_dir).mkdir(parents=True,exist_ok=True); s=SparkSession.builder.appName("unsw-nb15-etl").getOrCreate()
 try:
  raw=read_raw(s,[str(x) for x in files]).cache(); all_rows=typed_and_classified(raw).cache(); total=all_rows.count(); accepted=all_rows.filter(F.col("rejection_reason")=="").cache(); rejected=all_rows.filter(F.col("rejection_reason")!="").cache(); ok,bad=accepted.count(),rejected.count(); assert total==ok+bad,(total,ok,bad)
  accepted.repartition(16,"event_date").write.mode("overwrite").option("compression","snappy").partitionBy("event_date").parquet(a.processed_dir)
  rejected.write.mode("overwrite").option("compression","snappy").parquet(str(Path(a.artifact_dir)/"quarantine")); persisted=s.read.parquet(a.processed_dir).count(); assert persisted==ok,(persisted,ok)
  parts=[x for x in Path(a.processed_dir).glob("event_date=*") if x.is_dir()]; size=sum(x.stat().st_size for x in Path(a.processed_dir).rglob("*") if x.is_file())
  m={"created_at_utc":datetime.now(timezone.utc).isoformat(),"input_rows":total,"accepted_rows":ok,"rejected_rows":bad,"persisted_rows":persisted,"duplicate_rows_retained":total-raw.drop("_corrupt_record").dropDuplicates().count(),"source_hashes":[{"name":x.name,"sha256":sha(x),"bytes":x.stat().st_size} for x in files],"raw_schema":{"columns":COLUMNS,"read_as":"string; headerless"},"canonical_schema":{x.name:x.dataType.simpleString() for x in accepted.schema},"partitioning":{"columns":["event_date"],"partitions":len(parts),"write_repartition":16,"reason":"time partitioning supports range queries; category and IP partitions are skew/high-cardinality"},"storage_bytes":size,"rejection_counts":{r.rejection_reason:r["count"] for r in rejected.groupBy("rejection_reason").count().collect()},"category_normalization":{r.attack_cat_canonical:r["count"] for r in all_rows.groupBy("attack_cat_canonical").count().collect()},"timestamp_range":accepted.agg(F.min("stime").alias("min"),F.max("ltime").alias("max")).first().asDict()}
  json.dump(m,(Path(a.artifact_dir)/"etl_manifest.json").open("w"),indent=2,default=str); print(f"ETL PASSED: input={total:,} accepted={ok:,} rejected={bad:,} parquet={persisted:,}")
 finally: s.stop()
if __name__=="__main__": main()
