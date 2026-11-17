#!/usr/bin/env bash
set -euo pipefail

# Docker creates named-volume roots as root. Hadoop runs as the hadoop user,
# so prepare the two persistent directories before any daemon starts.
mkdir -p /data/dfs/name /data/dfs/data
chown -R hadoop:hadoop /data/dfs/name /data/dfs/data
chmod 700 /data/dfs/name /data/dfs/data
