#!/bin/bash

output=/home/eric/streaming/wordcount

spark-submit \
    --class com.eric.lab.spark.runner.streaming.SocketWordCountStreamingRunner \
    --master local[2] \
    --jars ../fastjson-1.2.51.jar \
    --conf spark.eventLog.enabled=true \
    ./sparklab-1.0-SNAPSHOT.jar \
    $input_path $output

