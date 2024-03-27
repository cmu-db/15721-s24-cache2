SERVER_ROOT="${SERVER_ROOT:-"."}" 
redis-server $SERVER_ROOT/redis.conf --port 6379 --cluster-config-file node1.conf&
redis-server $SERVER_ROOT/redis.conf --port 6380 --cluster-config-file node2.conf&
redis-server $SERVER_ROOT/redis.conf --port 6381 --cluster-config-file node3.conf&
sleep 5
redis-cli --cluster create localhost:6379 localhost:6380 localhost:6381 --cluster-replicas 0 --cluster-yes
REDIS_PORT=6379 cargo run &
REDIS_PORT=6380 cargo run &
REDIS_PORT=6381 cargo run &