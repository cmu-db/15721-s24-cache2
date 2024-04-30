#!/bin/bash
# The ports where Redis nodes are running
ports=(6379)
NODE_1_IP="${NODE_1_IP:-"localhost"}":6379
NODE_2_IP="${NODE_2_IP:-"localhost"}":6379
NODE_3_IP="${NODE_3_IP:-"localhost"}":6379

# Loop over each port and flush all databases then perform a hard reset on the cluster
for port in "${ports[@]}"
do
  echo "Flushing all databases on port $port..."
  redis-cli -p "$port" FLUSHALL
done

for port in "${ports[@]}"
do
  echo "Performing a hard reset on the cluster node running on port $port..."
  redis-cli -p "$port" CLUSTER RESET HARD
done

echo "All specified Redis nodes have been flushed and had a hard reset."

# Directory to store logs
LOG_DIR="./logs"
mkdir -p "${LOG_DIR}"

# Server root directory
SERVER_ROOT="${SERVER_ROOT:-"."}" 

# Names or keywords to identify your processes
PROCESS_NAMES=("istziio_server_node" "redis-serveredis-serverr")

# Kill existing processes based on the names or keywords
for name in "${PROCESS_NAMES[@]}"; do
    pkill -f $name
done

echo "Existing processes killed, if any were running."

# Start Redis instances
redis-server $SERVER_ROOT/redis.conf --port 6379 --cluster-config-file node1.conf&

echo "Redis servers starting..."

sleep 5
if [ $# -eq 1 ]; then
    redis-cli --cluster create $NODE_1_IP $NODE_2_IP $NODE_3_IP --cluster-replicas 0 --cluster-yes
fi
# Creating the cluster

sleep 5
echo "Redis cluster created."

# Starting the application servers
REDIS_PORT=6379 cargo run --bin istziio_server_node --\
  --bucket "istziio-bucket" \
  --region "us-east-1" \
  --access-key "$AWS_ACCESS_KEY_ID" \
  --secret-key "$AWS_SECRET_ACCESS_KEY" > "${LOG_DIR}/app_6379.log" 2>&1 &

echo "Application servers starting..."

# Reminder to check logs
echo "Check ${LOG_DIR} for logs."