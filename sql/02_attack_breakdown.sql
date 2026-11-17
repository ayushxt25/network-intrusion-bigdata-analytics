WITH counts AS (SELECT attack_cat_canonical, proto, service, COUNT(*) flows, SUM(sbytes+dbytes) bytes FROM flows WHERE label=1 GROUP BY 1,2,3)
SELECT c.*, r.reference_source FROM counts c JOIN attack_category_reference r ON c.attack_cat_canonical=r.attack_cat ORDER BY flows DESC LIMIT {{TOP_N}}
