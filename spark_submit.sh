#!/bin/bash

docker exec -it $(docker ps --filter "name=spark-master" -q) spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-common:3.4.0,com.amazonaws:aws-java-sdk:1.11.469 \
jobs/spark-city.py
