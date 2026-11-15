import shutil
import tempfile
import unittest
from pyspark.sql import SparkSession
from processing.unsw_nb15_common import COLUMNS, SCHEMA, typed_and_classified

def row(**values):
    data={c:"0" for c in COLUMNS}; data.update({"srcip":"10.0.0.1","dstip":"10.0.0.2","proto":"tcp","state":"FIN","service":"-","dur":"1.5","Sload":"2.5","Dload":"3.5","Sjit":"0","Djit":"0","Sintpkt":"0","Dintpkt":"0","tcprtt":"0","synack":"0","ackdat":"0","Stime":"1421927414","Ltime":"1421927415","label":"0","attack_cat":"None","sport":"80","dsport":"0x1bb", "_corrupt_record":None}); data.update(values); return tuple(data[c] for c in COLUMNS+["_corrupt_record"])

class UnswRulesTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls): cls.spark=SparkSession.builder.master("local[2]").appName("unsw-rules").config("spark.ui.enabled","false").getOrCreate()
 @classmethod
 def tearDownClass(cls): cls.spark.stop()
 def test_normalization_conversion_and_temporal_logic(self):
    df=typed_and_classified(self.spark.createDataFrame([row(),row(label="1",attack_cat=" Backdoors ",sport="-",dsport="70000"),row(Ltime="1421927413")],SCHEMA))
    values=df.select("attack_cat_canonical","sport","dsport","sport_raw","rejection_reason").collect()
    normal=next(v for v in values if v.attack_cat_canonical=="Normal" and v.rejection_reason==""); backdoor=next(v for v in values if v.attack_cat_canonical=="Backdoor"); temporal=next(v for v in values if "ltime_before_stime" in v.rejection_reason)
    self.assertEqual(normal.sport,80); self.assertEqual(normal.dsport,443)
    self.assertIsNone(backdoor.sport); self.assertIsNone(backdoor.dsport)
    self.assertIn("ltime_before_stime",temporal.rejection_reason)
 def test_reconciliation_and_parquet_roundtrip(self):
    frame=typed_and_classified(self.spark.createDataFrame([row(),row(Ltime="1421927413")],SCHEMA)); accepted=frame.filter("rejection_reason = ''"); rejected=frame.filter("rejection_reason <> ''")
    self.assertEqual(frame.count(),accepted.count()+rejected.count())
    target=tempfile.mkdtemp(prefix="unsw-parquet-")
    try:
      accepted.write.mode("overwrite").option("compression","snappy").parquet(target); self.assertEqual(self.spark.read.parquet(target).count(),accepted.count())
    finally: shutil.rmtree(target,ignore_errors=True)
if __name__=="__main__": unittest.main()
