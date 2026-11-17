SELECT event_date, label, attack_cat_canonical, COUNT(*) flows, SUM(sbytes+dbytes) bytes
FROM flows GROUP BY event_date,label,attack_cat_canonical ORDER BY event_date,label,flows DESC
