#!/bin/bash

LOG_FILE="/logs/container_health.log"

check_container() {
    container_name=$1
    if docker ps --filter "name=$container_name" --filter "status=running" | grep "$container_name" > /dev/null; then
        echo "$(date): $container_name is running." >> $LOG_FILE
        return 0
    else
        echo "$(date): $container_name is not running." >> $LOG_FILE
        return 1
    fi
}

check_spark_worker() {
    master_url="http://spark-master:8080"
    worker_name=$1

    workers=$(curl -s "${master_url}/json" | jq -r '.workers[].id')

    if echo "$workers" | grep -q "$worker_name"; then
        echo "$(date): $worker_name is registered with Spark Master." >> $LOG_FILE
        return 0
    else
        echo "$(date): $worker_name is NOT registered with Spark Master." >> $LOG_FILE
        return 1
    fi
}

# Redis 상태 확인
check_container "redis"

# Spark Master 상태 확인
check_container "spark-master"

# Spark Worker 상태 확인
check_container "spark-worker" && check_spark_worker "spark-worker"
