#!/bin/bash


set -e

echo "$(date +'%F %T') 🔧 Lancement du job Spark consumer..."

/opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:9.0.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-s3:1.12.661 \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/comsummer.py


exec "$@"