# Apache HBase кластер во Docker

1. `./start-containers.sh`
2. `./start-hadoop.sh`
3. `cd $HBASE_HOME/bin && ./start-hbase.sh`
4. `cd ../data-prep && java -jar hbase-data-prep-1.0-SNAPSHOT-jar-with-dependencies.jar` (внес на податоците)
5. `cd ../queries && java -jar hbase-queries-1.0-SNAPSHOT-jar-with-dependencies.jar` (извршување на прашалниците)

