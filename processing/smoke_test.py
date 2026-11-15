"""Distributed Spark/Spark SQL/Parquet foundation smoke test."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def run_smoke_checks(spark: SparkSession, output_path: str) -> None:
    """Assert a distributed aggregation, Spark SQL result, and Parquet round-trip."""
    rows = [
        ("attack", 20),
        ("normal", 10),
        ("attack", 25),
        ("normal", 15),
    ]
    events = spark.createDataFrame(rows, ["event_type", "bytes"]).repartition(2)

    aggregation = {
        row["event_type"]: (row["event_count"], row["total_bytes"])
        for row in events.groupBy("event_type").agg(
            F.count("*").alias("event_count"), F.sum("bytes").alias("total_bytes")
        ).collect()
    }
    assert aggregation == {"attack": (2, 45), "normal": (2, 25)}, aggregation

    events.createOrReplaceTempView("synthetic_events")
    sql_rows = spark.sql(
        """
        SELECT event_type, COUNT(*) AS event_count, SUM(bytes) AS total_bytes
        FROM synthetic_events
        GROUP BY event_type
        ORDER BY event_type
        """
    ).collect()
    assert [(r.event_type, r.event_count, r.total_bytes) for r in sql_rows] == [
        ("attack", 2, 45),
        ("normal", 2, 25),
    ]

    events.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
    round_trip = spark.read.parquet(output_path)
    assert round_trip.count() == len(rows)
    assert round_trip.groupBy().sum("bytes").first()[0] == 70


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()
    Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("network-intrusion-smoke-test").getOrCreate()
    try:
        run_smoke_checks(spark, args.output_path)
        print("SMOKE TEST PASSED: aggregation, Spark SQL, and Snappy Parquet round-trip.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
