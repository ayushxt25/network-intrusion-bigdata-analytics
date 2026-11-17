#!/usr/bin/env bash
set -euo pipefail

name_dir=/data/dfs/name
version_file="$name_dir/current/VERSION"

if [[ -f "$version_file" ]]; then
  exec hdfs namenode
fi

# An existing directory without VERSION is not safe to format automatically:
# it may contain incomplete or damaged metadata that needs operator review.
if [[ -n "$(find "$name_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "Refusing to format nonempty uninitialized NameNode directory: $name_dir" >&2
  exit 1
fi

hdfs namenode -format -nonInteractive
exec hdfs namenode
