"""Shared, non-destructive UNSW-NB15 schema and canonicalization rules."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

COLUMNS = ["srcip","sport","dstip","dsport","proto","state","dur","sbytes","dbytes","sttl","dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts","swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth","res_bdy_len","Sjit","Djit","Stime","Ltime","Sintpkt","Dintpkt","tcprtt","synack","ackdat","is_sm_ips_ports","ct_state_ttl","ct_flw_http_mthd","is_ftp_login","ct_ftp_cmd","ct_srv_src","ct_srv_dst","ct_dst_ltm","ct_src_ltm","ct_src_dport_ltm","ct_dst_sport_ltm","ct_dst_src_ltm","attack_cat","label"]
EXPECTED_FILES = [f"UNSW-NB15_{i}.csv" for i in range(1, 5)]
SCHEMA = StructType([StructField(c, StringType(), True) for c in COLUMNS] + [StructField("_corrupt_record", StringType(), True)])
FLOAT_COLUMNS = ["dur","Sload","Dload","Sjit","Djit","Sintpkt","Dintpkt","tcprtt","synack","ackdat"]
INT_COLUMNS = [c for c in COLUMNS if c not in {"srcip","sport","dstip","dsport","proto","state","service","attack_cat","label",*FLOAT_COLUMNS}]

def read_raw(spark, paths):
    return (spark.read.option("header", "false").option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").schema(SCHEMA).csv(paths))

def normalized(df):
    key=F.lower(F.trim(F.col("attack_cat")))
    category=(F.when((F.trim(F.col("label"))=="0") & (key.isNull() | key.isin("","none","null")),"Normal")
        .when(key.isin("backdoor","backdoors"),"Backdoor").when(key=="analysis","Analysis").when(key=="dos","DoS").when(key=="exploits","Exploits").when(key=="fuzzers","Fuzzers").when(key=="generic","Generic").when(key=="reconnaissance","Reconnaissance").when(key=="shellcode","Shellcode").when(key=="worms","Worms"))
    return df.withColumn("label_raw",F.col("label")).withColumn("attack_cat_raw",F.col("attack_cat")).withColumn("attack_cat_canonical",category)

def port_value(raw):
    value=(F.when(F.trim(raw).rlike(r"^[0-9]+$"),F.trim(raw).cast("long")).when(F.lower(F.trim(raw)).rlike(r"^0x[0-9a-f]+$"),F.conv(F.substring(F.trim(raw),3,64),16,10).cast("long")))
    return F.when(value.between(0,65535),value)

def typed_and_classified(df):
    frame=normalized(df); bad=[]
    for c in FLOAT_COLUMNS:
        bad.append(F.when(F.col(c).isNotNull() & (F.trim(F.col(c))!="") & F.expr(f"try_cast(`{c}` as double)").isNull(),f"invalid_{c}")); frame=frame.withColumn(c,F.expr(f"try_cast(`{c}` as double)"))
    for c in INT_COLUMNS:
        bad.append(F.when(F.col(c).isNotNull() & (F.trim(F.col(c))!="") & F.expr(f"try_cast(`{c}` as bigint)").isNull(),f"invalid_{c}")); frame=frame.withColumn(c,F.expr(f"try_cast(`{c}` as bigint)"))
    frame=frame.withColumn("sport_raw",F.col("sport")).withColumn("dsport_raw",F.col("dsport")).withColumn("sport",port_value(F.col("sport"))).withColumn("dsport",port_value(F.col("dsport")))
    frame=frame.withColumn("sport_parse_status",F.when(F.col("sport").isNotNull(),"valid").otherwise("special_or_invalid")).withColumn("dsport_parse_status",F.when(F.col("dsport").isNotNull(),"valid").otherwise("special_or_invalid"))
    frame=frame.withColumn("label",F.expr("try_cast(label_raw as int)")).withColumn("stime_epoch",F.expr("try_cast(Stime as bigint)")).withColumn("ltime_epoch",F.expr("try_cast(Ltime as bigint)"))
    frame=frame.withColumn("stime",F.expr("timestamp_seconds(stime_epoch)")).withColumn("ltime",F.expr("timestamp_seconds(ltime_epoch)")).withColumn("event_date",F.to_date("stime"))
    reasons=[F.when(F.col("_corrupt_record").isNotNull(),"malformed_record"),F.when(~F.trim(F.col("label_raw")).isin("0","1") | F.col("label_raw").isNull(),"invalid_label"),F.when(F.col("attack_cat_canonical").isNull(),"unknown_category"),F.when((F.col("label")==0)&(F.col("attack_cat_canonical")!="Normal"),"label_category_mismatch"),F.when((F.col("label")==1)&(F.col("attack_cat_canonical")=="Normal"),"label_category_mismatch"),F.when(F.col("stime_epoch").isNull()|F.col("ltime_epoch").isNull(),"invalid_timestamp"),F.when(F.col("ltime_epoch")<F.col("stime_epoch"),"ltime_before_stime")]+bad
    return frame.withColumn("rejection_reason",F.concat_ws("|",*reasons))
