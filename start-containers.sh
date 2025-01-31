#!/bin/bash

# the default node number is 3
N=${1:-3}


# start hadoop master container
docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
docker run -itd \
                --net=hadoop \
                -p 50070:50070 \
                -p 8088:8088 \
		-p 16010:16010 \
                --name hadoop-master \
                --hostname hadoop-master \
                hadoop-cluster-hbase:2.6.1-2.10.2-master &> /dev/null

# start hadoop slave container
i=1
while [ $i -lt $N ]
do
	docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-slave$i container..."
	docker run -itd \
	                --net=hadoop \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                hadoop-cluster-hbase:2.6.1-2.10.2 &> /dev/null
	i=$(( $i + 1 ))
done

docker cp hbase-conf/hbase-site-slave1.xml hadoop-slave1:/usr/local/hbase/conf/hbase-site.xml
docker cp hbase-conf/hbase-site-slave2.xml hadoop-slave2:/usr/local/hbase/conf/hbase-site.xml 

# get into hadoop master container
docker exec -it hadoop-master bash
