#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--check-args" ]]; then
  shift
  [[ "$#" -eq 2 ]] || { echo "expected two check arguments" >&2; exit 64; }
  printf 'ARG[0]=<%s>\nARG[1]=<%s>\n' "$1" "$2"
  exit 0
fi

[[ "$#" -eq 2 ]] || { echo "usage: $0 <hdfs-input-dir> <hdfs-output-dir>" >&2; exit 64; }
input_dir="$1"
output_dir="$2"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hadoop}"

build_dir="$(mktemp -d /tmp/unsw-category-demo.XXXXXX)"
trap 'rm -rf "$build_dir"' EXIT
mkdir -p "$build_dir/classes"

javac -cp "$(hadoop classpath)" -d "$build_dir/classes" hadoop/src/UnswMapReduce.java
jar cf "$build_dir/unsw-mapreduce-demo.jar" -C "$build_dir/classes" .
echo "JAVA_COMPILE_OK"
hadoop jar "$build_dir/unsw-mapreduce-demo.jar" UnswMapReduce category "$input_dir" "$output_dir"
