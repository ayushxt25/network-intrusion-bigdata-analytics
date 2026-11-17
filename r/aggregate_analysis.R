library(ggplot2); library(jsonlite)
out <- "artifacts/r"; dir.create(out,recursive=TRUE,showWarnings=FALSE)
readagg <- function(p,n){x<-read.delim(p,header=FALSE,sep="\t",stringsAsFactors=FALSE); names(x)<-n;x}
cat <- readagg("artifacts/hadoop/category/part-r-00000",c("category","flows")); proto <- readagg("artifacts/hadoop/protocol/part-r-00000",c("protocol","flows","bytes","attacks"))
png(file.path(out,"attack_categories.png"),1000,600); print(ggplot(cat,aes(reorder(category,flows),flows))+geom_col()+coord_flip()+labs(x="Attack category",title="UNSW-NB15 Hadoop aggregate")); dev.off()
png(file.path(out,"protocol_distribution.png"),1000,600); print(ggplot(head(proto[order(-proto$flows),],20),aes(reorder(protocol,flows),flows))+geom_col()+coord_flip()+labs(x="Protocol",title="Top protocols from Hadoop aggregate")); dev.off()
write_json(list(attack_categories=cat,protocols=proto,limitation="Aggregate counts cannot support flow-level covariance/correlation; use canonical Parquet numeric flows in a future R flow-level job."),file.path(out,"statistical_summary.json"),pretty=TRUE)
