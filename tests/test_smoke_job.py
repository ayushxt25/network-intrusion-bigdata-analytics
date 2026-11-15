import shutil
import tempfile
import unittest

from pyspark.sql import SparkSession

from processing.smoke_test import run_smoke_checks


class SmokeJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("network-intrusion-smoke-unit-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_sql_and_parquet_round_trip(self):
        output = tempfile.mkdtemp(prefix="spark-smoke-")
        try:
            run_smoke_checks(self.spark, output)
        finally:
            shutil.rmtree(output, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
