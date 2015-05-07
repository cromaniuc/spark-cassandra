1) Start cassandra /opt/apache-cassandra-2.0.14/bin$ sudo ./cassandra

2) Start Spark master /opt/spark-1.3.1-bin-hadoop2.6/bin$ ./spark-class org.apache.spark.deploy.master.Master

    3) Start Spark worker /opt/spark-1.3.1-bin-hadoop2.6/bin$ ./spark-class org.apache.spark.deploy.worker.Worker spark://<master_host>:7077

4) mvn clean package exec:java