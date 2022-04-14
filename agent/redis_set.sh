#!/bin/bash

# Default to '*' key pattern, meaning all redis keys in the namespace
REDIS_KEY_PATTERN="${REDIS_KEY_PATTERN:-*}"

# Connect to redis-cli
REDIS_HOST=`cat ../ansible/environments/distributed/hosts | grep -A 1 "\[edge\]" | grep "ansible_host" | awk {'print $1'}`
REDIS_PORT="6379"
REDIS_PASSWORD="openwhisk"

REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT -a $REDIS_PASSWORD"

$REDIS_CLI hmset predict_peak alu "32" knn "32"
$REDIS_CLI hmset predict_duration alu "2" knn "5"