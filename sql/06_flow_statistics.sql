SELECT label, attack_cat_canonical, COUNT(*) flows, AVG(dur) avg_duration, percentile_approx(dur,0.95) p95_duration, AVG(sbytes+dbytes) avg_bytes, percentile_approx(sbytes+dbytes,0.95) p95_bytes, AVG(Sload) avg_sload, AVG(Dload) avg_dload, AVG(CASE WHEN dur>0 THEN (sbytes+dbytes)/dur END) avg_derived_bytes_per_second
FROM flows GROUP BY label,attack_cat_canonical ORDER BY label,flows DESC
