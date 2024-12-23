#!/bin/bash

check_container() {
    container_name=$1
    if docker ps --filter "name=$container_name" --filter "status=running" | grep "$container_name" > /dev/null; then
        echo "$container_name is running."
        return 0
    else
        echo "$container_name is not running."
        return 1
    fi
}

check_spark_worker() {
    master_url="http://spark-master:8080"
    worker_name=$1

    # Master의 Worker 목록 가져오기
    workers=$(curl -s "${master_url}/json" | jq -r '.workers[].id')

    if echo "$workers" | grep -q "$worker_name"; then
        echo "$worker_name is registered with Spark Master."
        return 0
    else
        echo "$worker_name is NOT registered with Spark Master."
        return 1
    fi
}

# Redis 상태 확인
check_container "redis"

# Spark Master 상태 확인
check_container "spark-master"

# Spark Worker 상태 확인
check_container "spark-worker" && check_spark_worker "spark-worker"