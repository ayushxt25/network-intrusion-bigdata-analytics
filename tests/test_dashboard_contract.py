import json
from pathlib import Path
import pandas as pd
root=Path("artifacts/analytics")
manifest=json.loads((root/"analytics_manifest.json").read_text())
assert manifest["source_rows"]==2540047 and len(manifest["queries"])==8
assert all((root / q["name"]).is_dir() for q in manifest["queries"])
traffic=pd.read_parquet(list((root/"01_traffic_distribution").glob("*.parquet")))
totals=traffic.groupby("label")["flows"].sum().to_dict()
assert totals=={0:2218764,1:321283}
assert "rules are inferred" in Path("dashboard/app.py").read_text()
