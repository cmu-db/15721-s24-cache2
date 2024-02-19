# 15721-s24-cache2
15-721 Spring 2024 - Cache #2
# LRU Cache Server

This server implements a Least Recently Used (LRU) caching mechanism, providing a simple interface for fetching files from a simulated S3 storage and managing them within an LRU cache. The server is built using Rust and the Rocket framework.

## Features

- **Health Check**: Verify the server's health.
- **Fetch File**: Retrieve files, either served from the cache or fetched from "S3" and then cached.
- **Cache Stats**: Get statistics about the current state of the cache.
- **Set Cache Size**: Adjust the maximum size of the cache dynamically.

## Getting Started

### Prerequisites

- Rust and Cargo (latest stable version recommended)
- Rocket Framework

### Installation

1. Clone the repository:
    ```sh
    git clone <repository-url>
    cd <repository-name>/<server>
    ```

2. Build the project:
    ```sh
    cargo build --release
    ```

3. Run the server:
    ```sh
    cargo run --release
    ```

The server will start, and by default, it listens on `http://localhost:8000`.

## Usage

### Health Check

- **Endpoint**: `GET /`
- **Description**: Checks if the server is running.
- **CURL Command**:
    ```sh
    curl http://localhost:8000/
    ```

### Fetch File

- **Endpoint**: `GET /s3/<path>`
- **Description**: Retrieves a file from the cache or fetches it from the simulated S3 storage if not present in the cache. Error reports if file not existed.
- **CURL Command**:
    ```sh
    curl http://localhost:8000/s3/<path-to-file>
    ```

### Cache Stats

- **Endpoint**: `GET /stats`
- **Description**: Returns statistics about the cache, such as current size, maximum size, and number of entries.
- **CURL Command**:
    ```sh
    curl http://localhost:8000/stats
    ```

### Set Cache Size

- **Endpoint**: `POST /size/<new_size>`
- **Description**: Adjusts the maximum size of the cache.
- **CURL Command**:
    ```sh
    curl -X POST http://localhost:8000/size/<new-size-in-bytes>
    ```