#!/bin/bash

spark-submit \
    --class com.eric.lab.spark.runner.streaming.DirectKafkaRunner \
    --master local[2] \
    --jars ./fastjson-1.2.51.jar,./kafka-clients-0.10.0.1.jar,./spark-streaming-kafka-0-10_2.11-2.2.1.jar \
    --conf spark.eventLog.enabled=true \
    ./sparklab-1.0-SNAPSHOT.jar \
