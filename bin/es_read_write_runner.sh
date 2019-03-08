#!/bin/bash

spark-submit \
    --class com.eric.lab.spark.runner.outputformat.ElasticReaderWriterRunner \
    --master local[2] \
    --executor-memory 1g \
    --driver-memory 1g \
    --jars ./fastjson-1.2.51.jar,./mysql-connector-java-8.0.12.jar,./elasticsearch-hadoop-6.6.1.jar \
    --conf spark.eventLog.enabled=true \
    ./sparklab-1.0-SNAPSHOT.jar \
