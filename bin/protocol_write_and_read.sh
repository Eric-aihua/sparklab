#!/bin/bash

rm -rf /tmp/sparklab/pbtest

spark-submit \
    --class com.eric.lab.spark.runner.outputformat.ProtocolBufferWriterRunner \
    --master local[2] \
    --executor-memory 1g \
    --driver-memory 1g \
    --jars ./fastjson-1.2.51.jar,./protobuf-java-2.5.0.jar\
    --conf spark.eventLog.enabled=true \
    ./sparklab-1.0-SNAPSHOT.jar \


spark-submit \
    --class com.eric.lab.spark.runner.outputformat.ProtocolBufferReaderRunner \
    --master local[2] \
    --executor-memory 1g \
    --driver-memory 1g \
    --jars ./fastjson-1.2.51.jar,./protobuf-java-2.5.0.jar\
    --conf spark.eventLog.enabled=true \
    ./sparklab-1.0-SNAPSHOT.jar \