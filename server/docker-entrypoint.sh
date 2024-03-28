#!/bin/sh
echo "port ${REDIS_PORT}" >> /usr/local/bin/redis.conf
redis-server /usr/local/bin/redis.conf &
/usr/local/bin/istziio_server_node
