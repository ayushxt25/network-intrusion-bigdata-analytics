SELECT srcip,dstip,proto,service,COUNT(*) flows,SUM(label) observed_attack_flows,MIN(stime) first_seen,MAX(ltime) last_seen
FROM flows GROUP BY srcip,dstip,proto,service HAVING COUNT(*) >= 10 ORDER BY flows DESC LIMIT {{TOP_N}}
