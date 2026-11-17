SELECT label, attack_cat_canonical, COUNT(*) AS flows, SUM(sbytes + dbytes) AS bytes
FROM flows GROUP BY label, attack_cat_canonical ORDER BY label, flows DESC
