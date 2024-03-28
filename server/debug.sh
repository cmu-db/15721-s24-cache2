#!/bin/bash

# Stop and remove containers, networks
docker-compose down || { echo "docker-compose down failed"; exit 1; }

# Build your Rust project
cargo build || { echo "cargo build failed"; exit 1; }

# Build your Docker image
docker build -t istziio . || { echo "docker build failed"; exit 1; }

# Start up your Docker containers in the background
docker-compose up -d || { echo "docker-compose up -d failed"; exit 1; }

# Execute a bash shell inside your specified container
# Note: You might need to adjust the container name based on your docker-compose settings
docker exec -it server-servernode_1-1 /bin/bash || { echo "docker exec failed"; exit 1; }
