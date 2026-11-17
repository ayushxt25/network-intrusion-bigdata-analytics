import unittest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

class AnalyticsTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.spark=SparkSession.builder.master("local[2]").appName("analytics-tests").config("spark.ui.enabled","false").getOrCreate()
  t=datetime(2015,1,22,0,0)
  rows=[("a","d","tcp","http",t,t+timedelta(seconds=1),0,"Normal",100,0,1,0,1.0,100.0,0.0),("a","d","tcp","http",t+timedelta(seconds=300),t+timedelta(seconds=301),1,"Exploits",200,0,2,0,1.0,200.0,0.0),("a","d2","udp","dns",t+timedelta(seconds=301),t+timedelta(seconds=302),0,"Normal",300,0,3,0,1.0,300.0,0.0)]
  cls.spark.createDataFrame(rows,"srcip string,dstip string,proto string,service string,stime timestamp,ltime timestamp,label int,attack_cat_canonical string,sbytes long,dbytes long,Spkts long,Dpkts long,dur double,Sload double,Dload double").createOrReplaceTempView("flows")
  cls.spark.createDataFrame([("Exploits","UNSW-NB15 published attack taxonomy")],"attack_cat string,reference_source string").createOrReplaceTempView("attack_category_reference")
 @classmethod
 def tearDownClass(cls): cls.spark.stop()
 def test_group_by_and_reference_join(self):
  rows=self.spark.sql("SELECT f.attack_cat_canonical,COUNT(*) n,r.reference_source FROM flows f JOIN attack_category_reference r ON f.attack_cat_canonical=r.attack_cat GROUP BY 1,3").collect(); self.assertEqual(rows[0].n,1)
 def test_window_boundary(self):
  q="SELECT unix_timestamp(stime) x,COUNT(*) OVER(PARTITION BY srcip ORDER BY unix_timestamp(stime) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) n FROM flows ORDER BY x"; self.assertEqual([r.n for r in self.spark.sql(q).collect()],[1,2,2])
 def test_label_and_inferred_rule(self):
  self.assertEqual(self.spark.sql("SELECT COUNT(*) n FROM flows WHERE label=1 AND attack_cat_canonical='Exploits'").first().n,1)
  self.assertEqual(self.spark.sql("SELECT COUNT(*) n FROM flows WHERE label=0 GROUP BY srcip HAVING COUNT(*)>=2").first().n,2)
if __name__=="__main__": unittest.main()
