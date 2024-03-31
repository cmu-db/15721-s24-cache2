#!/bin/bash
# The ports where Redis nodes are running
ports=(6379 6380 6381)

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
SERVER_ROOT="$(cd "$(dirname "${SERVER_ROOT:-"."}")"; pwd)/$(basename "${SERVER_ROOT:-"."}")"
echo "server root set to: $SERVER_ROOT"

# Names or keywords to identify your processes
PROCESS_NAMES=("istziio_server_node" "redis-serveredis-serverr")

# Kill existing processes based on the names or keywords
for name in "${PROCESS_NAMES[@]}"; do
    pkill -f $name
done

echo "Existing processes killed, if any were running."

rm $SERVER_ROOT/node1.conf $SERVER_ROOT/node2.conf $SERVER_ROOT/node3.conf

# Start Redis instances
redis-server $SERVER_ROOT/redis.conf --port 6379 --cluster-config-file $SERVER_ROOT/node1.conf&
redis-server $SERVER_ROOT/redis.conf --port 6380 --cluster-config-file $SERVER_ROOT/node2.conf&
redis-server $SERVER_ROOT/redis.conf --port 6381 --cluster-config-file $SERVER_ROOT/node3.conf&

echo "Redis servers starting..."

sleep 5

# Creating the cluster
redis-cli --cluster create localhost:6379 localhost:6380 localhost:6381 --cluster-replicas 0 --cluster-yes

docker container stop istziio_test_mock_s3 > /dev/null 2>&1
docker container rm istziio_test_mock_s3 > /dev/null 2>&1
docker run -v $SERVER_ROOT/tests/test_s3_files:/usr/share/nginx/html -p 6333:80 --name istziio_test_mock_s3 -d nginx