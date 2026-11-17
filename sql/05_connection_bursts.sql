WITH ordered AS (SELECT srcip,dstip,stime,label,attack_cat_canonical, COUNT(*) OVER (PARTITION BY srcip ORDER BY unix_timestamp(stime) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) AS flows_in_prior_5m FROM flows), bursts AS (SELECT * FROM ordered WHERE flows_in_prior_5m >= 50)
SELECT * FROM bursts ORDER BY flows_in_prior_5m DESC, stime LIMIT {{TOP_N}}
