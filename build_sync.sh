#!/bin/bash

mvn clean
mvn package -DskipTests
scp bin/*.* root@192.168.80.130:~/sc/spark_cb
scp target/sparklab-1.0-SNAPSHOT.jar root@192.168.80.130:~/sc/spark_cb

