HELP;
TABLES;
SCHEMA;
SELECT COUNT(*) AS total_records FROM network_flows;
SELECT SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS observed_attacks, SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) AS normal_flows FROM network_flows;
SELECT attack_cat_canonical, COUNT(*) AS attack_flows FROM network_flows WHERE label = 1 GROUP BY attack_cat_canonical ORDER BY attack_flows DESC LIMIT 5;
SELECT
  proto,
  COUNT(*) AS flows
FROM network_flows
GROUP BY proto
ORDER BY flows DESC
LIMIT 3;
SELECT not_a_real_column FROM network_flows;
WITH ranked AS (
  SELECT proto, COUNT(*) AS attack_flows, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM network_flows WHERE label = 1 GROUP BY proto
)
SELECT proto, attack_flows FROM ranked WHERE rank <= 3 ORDER BY rank;
SELECT COUNT(*) AS records_after_error FROM network_flows;
