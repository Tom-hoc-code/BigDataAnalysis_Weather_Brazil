#!/bin/bash

set -e

echo "====== SPARK START SCRIPT ======"

echo "SPARK_MODE: $SPARK_MODE"
echo "SPARK_MASTER: $SPARK_MASTER"

SPARK_HOME=/usr/local/gr2/spark

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    $SPARK_HOME/sbin/start-master.sh

    echo "Starting FastAPI..."
    uvicorn main:app --host 0.0.0.0 --port 8000 &

elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
else
    echo "Invalid SPARK_MODE"
    exit 1
fi

sleep 5

echo "====== SPARK PROCESSES ======"
ps -ef | grep -E "spark|uvicorn"

tail -f /dev/null