"""Phase 1 Streamlit service health page; no analytics are implemented yet."""

import os
from pathlib import Path

import streamlit as st


st.set_page_config(page_title="Intrusion Analytics | Health", page_icon="🛡️", layout="centered")
st.title("Network Intrusion Analytics")
st.subheader("Phase 1 service health")
st.success("Dashboard service is running.")

spark_url = os.getenv("SPARK_MASTER_URL", "not configured")
fixture = Path(os.getenv("SMOKE_PARQUET_PATH", "artifacts/smoke_parquet"))
st.write(f"Spark master target: `{spark_url}`")
st.write(f"Smoke Parquet fixture: {'available' if fixture.exists() else 'not generated yet'}")
st.caption("Dataset ingestion, analytical queries, charts, and benchmarks are intentionally deferred.")
