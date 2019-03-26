#!/bin/bash

rm -rf /tmp/sparklab/compresstest

spark-submit \
    --class com.eric.lab.spark.runner.advance.PipeSampleRunner \
    --master local[2] \
    --executor-memory 1g \
    --driver-memory 1g \
    --jars ./fastjson-1.2.51.jar,./protobuf-java-2.5.0.jar\
    --conf spark.eventLog.enabled=true \
    ./sparklab-1.0-SNAPSHOT.jar \
