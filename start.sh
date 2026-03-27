#!/bin/bash

set -e

echo "====== SPARK START SCRIPT ======"

echo "SPARK_MODE: $SPARK_MODE"
echo "SPARK_MASTER: $SPARK_MASTER"

SPARK_HOME=/usr/local/gr2/spark

if [ "$SPARK_MODE" = "master" ]; then
    $SPARK_HOME/sbin/start-master.sh
elif [ "$SPARK_MODE" = "worker" ]; then
    $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
else
    echo "Invalid SPARK_MODE"
    exit 1
fi

sleep 5

echo "====== SPARK PROCESSES ======"
ps -ef | grep spark

tail -f /dev/null