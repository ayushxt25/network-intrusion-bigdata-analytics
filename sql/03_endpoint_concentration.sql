SELECT srcip, COUNT(*) flows, SUM(label) observed_attack_flows, COUNT(DISTINCT dstip) destinations, SUM(sbytes+dbytes) bytes
FROM flows GROUP BY srcip HAVING COUNT(*) >= 10 ORDER BY flows DESC LIMIT {{TOP_N}}
