"""Precomputed Phase 3 Spark analytics dashboard; it never scans raw data."""
import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
st.set_page_config(page_title="Intrusion Analytics",layout="wide")
ROOT=Path("/app/artifacts/analytics")
def table(name):
 files=list((ROOT/name).rglob("*.parquet")); return pd.read_parquet(files) if files else pd.DataFrame()
st.title("Large-Scale Network Intrusion Analytics")
manifest=ROOT/"analytics_manifest.json"
if not manifest.exists(): st.warning("Run Phase 3 Spark analytics to publish artifacts."); st.stop()
m=json.loads(manifest.read_text()); st.caption("Precomputed Spark SQL only. label=1 is observed; rules are inferred evidence.")
traffic=table("01_traffic_distribution")
if not traffic.empty:
 totals=traffic.groupby("label",as_index=True)["flows"].sum(); normal=int(totals.get(0,0)); attacks=int(totals.get(1,0)); assert normal+attacks==m["source_rows"]
 c1,c2,c3=st.columns(3); c1.metric("Canonical flows",f"{m['source_rows']:,}"); c2.metric("Observed normal flows",f"{normal:,}"); c3.metric("Observed attack-labelled flows",f"{attacks:,}")
 st.plotly_chart(px.bar(traffic,x="attack_cat_canonical",y="flows",color="label",title="Observed normal vs attack-labelled traffic"),use_container_width=True)
for name,title,x,y in [("02_attack_breakdown","Attack categories","attack_cat_canonical","flows"),("03_endpoint_concentration","Endpoint concentration","srcip","flows"),("04_time_trends","Temporal trends","event_date","flows"),("05_connection_bursts","Connection bursts","srcip","flows_in_prior_5m"),("07_repeated_communications","Repeated communications","srcip","flows"),("08_pattern_rules","Rule-derived suspicious evidence","srcip","evidence_flows")]:
 d=table(name)
 if not d.empty: st.subheader(title); st.plotly_chart(px.bar(d.head(50),x=x,y=y,color="rule_name" if "rule_name" in d else None),use_container_width=True); st.dataframe(d.head(50),use_container_width=True)
st.subheader("Flow statistics"); st.dataframe(table("06_flow_statistics"),use_container_width=True)
st.subheader("Available performance metadata"); st.json(m)
