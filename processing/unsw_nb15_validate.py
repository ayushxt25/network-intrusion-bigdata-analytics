"""Non-destructive UNSW-NB15 validation report."""
import argparse, hashlib, json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from processing.unsw_nb15_common import COLUMNS, EXPECTED_FILES, read_raw, normalized, typed_and_classified

def digest(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1048576),b""): h.update(b)
    return h.hexdigest()
def main():
    p=argparse.ArgumentParser(); p.add_argument("--input-dir",default="/workspace/data/raw/UNSW-NB15"); p.add_argument("--report-dir",default="/workspace/artifacts/unsw_nb15_validation"); a=p.parse_args()
    source,out=Path(a.input_dir),Path(a.report_dir); out.mkdir(parents=True,exist_ok=True); files=[source/n for n in EXPECTED_FILES]
    meta={"dataset":"UNSW-NB15","validated_at_utc":datetime.now(timezone.utc).isoformat(),"expected_files":EXPECTED_FILES,"header":False,"column_count":49,"columns":COLUMNS,"raw_data_modified":False}
    if not all(x.is_file() for x in files):
        meta.update(status="INPUT_MISSING",missing_files=[x.name for x in files if not x.is_file()]); json.dump(meta,(out/"unsw_nb15_metadata.json").open("w"),indent=2); print("UNSW-NB15 validation: INPUT_MISSING"); return
    s=SparkSession.builder.appName("unsw-nb15-validation").getOrCreate()
    try:
        raw=read_raw(s,[str(x) for x in files]).cache(); typed=typed_and_classified(raw).cache(); rows=raw.count()
        cats={r.attack_cat_canonical:r["count"] for r in normalized(raw).groupBy("attack_cat_canonical").count().collect()}
        meta.update(status="VALIDATED",record_count=rows,files=[{"name":x.name,"bytes":x.stat().st_size,"sha256":digest(x)} for x in files],duplicate_records=rows-raw.drop("_corrupt_record").dropDuplicates().count(),label_distribution={str(r.label_raw):r["count"] for r in typed.groupBy("label_raw").count().collect()},category_normalized_distribution=cats,invalid_ports={"sport":typed.filter(F.col("sport").isNull()&F.col("sport_raw").isNotNull()).count(),"dsport":typed.filter(F.col("dsport").isNull()&F.col("dsport_raw").isNotNull()).count()},rejection_counts={r.rejection_reason:r["count"] for r in typed.filter(F.col("rejection_reason")!="").groupBy("rejection_reason").count().collect()},timestamp_range=typed.agg(F.min("stime").alias("min"),F.max("ltime").alias("max")).first().asDict())
        json.dump(meta,(out/"unsw_nb15_metadata.json").open("w"),indent=2,default=str); (out/"unsw_nb15_quality_report.md").write_text(f"# UNSW-NB15 validation\n\nRecords: {rows:,}. Normalized categories: {cats}.\n")
        print(f"UNSW-NB15 validation: VALIDATED ({rows:,} rows).")
    finally: s.stop()
if __name__=="__main__": main()
