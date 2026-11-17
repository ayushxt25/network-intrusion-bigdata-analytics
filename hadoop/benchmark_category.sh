#!/usr/bin/env bash
set -euo pipefail; export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hadoop}"
mkdir -p build artifacts/benchmarks
timings="artifacts/benchmarks/hadoop_category_timings.jsonl"
[[ ! -e "$timings" ]] || { echo "Refusing to append timings to an existing benchmark run: $timings" >&2; exit 1; }
javac -cp "$(hadoop classpath)" -d build hadoop/src/UnswMapReduce.java; jar cf build/unsw-mapreduce.jar -C build .
for i in 1 2 3; do start=$(date +%s%N); out="/data/unsw/benchmarks/category-$i"; hdfs dfs -test -e "$out" && { echo "Refusing existing benchmark output $out" >&2; exit 1; }; hadoop jar build/unsw-mapreduce.jar UnswMapReduce category /data/unsw/raw "$out"; end=$(date +%s%N); elapsed=$(awk "BEGIN {print ($end-$start)/1000000000}"); printf '{"iteration":%s,"elapsed_seconds":%.6f}\n' "$i" "$elapsed" >> "$timings"; done
