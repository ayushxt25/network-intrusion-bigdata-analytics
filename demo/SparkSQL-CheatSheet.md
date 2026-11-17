# Spark SQL viva cheat sheet

Start: `./demo/Start-SparkSQL.ps1` then end every SQL statement with `;`.

```sql
-- 1. Total records
SELECT COUNT(*) AS total_records FROM network_flows;

-- 2. Normal versus observed attack-labelled flows
SELECT label, COUNT(*) AS flows FROM network_flows GROUP BY label ORDER BY label;

-- 3. Top five attack categories
SELECT attack_cat_canonical, COUNT(*) AS flows FROM network_flows
WHERE label = 1 GROUP BY attack_cat_canonical ORDER BY flows DESC LIMIT 5;

-- 4. Top ten protocols by attack count
SELECT proto, COUNT(*) AS attacks FROM network_flows WHERE label = 1
GROUP BY proto ORDER BY attacks DESC LIMIT 10;

-- 5. Top ten source IPs by traffic volume
SELECT srcip, SUM(sbytes + dbytes) AS bytes FROM network_flows
GROUP BY srcip ORDER BY bytes DESC LIMIT 10;

-- 6. Average connection duration by category
SELECT attack_cat_canonical, AVG(dur) AS avg_duration FROM network_flows
GROUP BY attack_cat_canonical ORDER BY avg_duration DESC;

-- 7. Filter a protocol (change tcp as needed)
SELECT srcip, dstip, service, dur, sbytes, dbytes, label FROM network_flows
WHERE lower(proto) = 'tcp' LIMIT 20;

-- 8. Distinct source IPs
SELECT COUNT(DISTINCT srcip) AS distinct_source_ips FROM network_flows;

-- 9. Attack percentage by protocol
SELECT proto, COUNT(*) AS flows, SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS attacks,
       ROUND(100.0 * SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS attack_pct
FROM network_flows GROUP BY proto ORDER BY attack_pct DESC, flows DESC LIMIT 10;

-- 10. CTE plus window function
WITH protocol_counts AS (
  SELECT proto, COUNT(*) AS attack_flows,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM network_flows WHERE label = 1 GROUP BY proto
)
SELECT proto, attack_flows, rank FROM protocol_counts WHERE rank <= 10 ORDER BY rank;
```

Use `SCHEMA`, `TABLES`, `HELP`, `CLEAR`, and `EXIT` without a semicolon. The console is read-only and displays at most 50 rows by default; it does not automatically count all matching rows.
