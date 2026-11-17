#!/usr/bin/env bash
set -euo pipefail
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hadoop}"

for attempt in $(seq 1 60); do
  if hdfs dfs -ls / >/dev/null 2>&1 \
    && hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1):' \
    && yarn node -list 2>/dev/null | grep -q 'RUNNING'; then
    break
  fi
  if [[ "$attempt" == 60 ]]; then
    echo "Timed out waiting for HDFS DataNode and YARN NodeManager readiness" >&2
    exit 1
  fi
  sleep 2
done

mkdir -p build artifacts/hadoop
javac -cp "$(hadoop classpath)" -d build hadoop/src/UnswMapReduce.java
jar cf build/unsw-mapreduce.jar -C build .
hdfs dfs -mkdir -p /data/unsw/raw
hdfs dfs -put -f data/raw/UNSW-NB15/UNSW-NB15_*.csv /data/unsw/raw/
for job in category protocol source; do hdfs dfs -rm -r -f /data/unsw/out/$job || true; hadoop jar build/unsw-mapreduce.jar UnswMapReduce "$job" /data/unsw/raw /data/unsw/out/$job; rm -rf artifacts/hadoop/$job; hdfs dfs -get /data/unsw/out/$job artifacts/hadoop/$job; done
