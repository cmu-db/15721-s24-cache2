# ISTSIIO

## Server
To run Redis server, use Docker to start a container:
`docker run --name=redis-devel --publish=6379:6379 --hostname=redis --restart=on-failure --detach redis:latest`

## Detailed Design 
### Client-Server Flow
```mermaid
sequenceDiagram
    participant Client
    participant RedisServer
    participant RedisCluster
    participant FileSystem

    Client->>RedisServer: new(addrs)
    RedisServer->>RedisCluster: Connect to Redis Cluster
    RedisCluster->>RedisServer: Connection established

    Client->>RedisServer: get_myid()
    alt myid is not cached
        RedisServer->>RedisCluster: Run 'redis-cli cluster myid'
        RedisCluster->>RedisServer: Return myid
    end

    Client->>RedisServer: location_lookup(uid)
    RedisServer->>RedisCluster: Determine which slot uid belongs to
    RedisCluster->>RedisServer: Return node information
    RedisServer->>Client: Return endpoint or None

    Client->>RedisServer: get_file(uid)
    alt File is cached
        RedisServer->>FileSystem: Retrieve file from local cache
        FileSystem->>RedisServer: Return file
    else File is not cached
        RedisServer->>RedisCluster: Fetch from S3
        RedisCluster->>FileSystem: Store file in local cache
        FileSystem->>RedisServer: Return file
    end

    Client->>RedisServer: set_file_cache_loc(uid, loc)
    RedisServer->>RedisCluster: Set cache location in Redis
    RedisCluster->>RedisServer: Acknowledge

    Client->>RedisServer: remove_file(uid)
    RedisServer->>RedisCluster: Remove cache location from Redis
    RedisCluster->>RedisServer: Acknowledge

    Client->>RedisServer: import_keyslot(keyslots)
    loop For each keyslot
        RedisServer->>RedisCluster: Set keyslot to NODE
        RedisCluster->>RedisServer: Acknowledge
    end

    Client->>RedisServer: migrate_keyslot_to(keyslots, node_id)
    loop For each keyslot
        RedisServer->>RedisCluster: Get keys in slot
        RedisCluster->>RedisServer: Return keys
        RedisServer->>RedisCluster: Delete keys
        RedisCluster->>RedisServer: Acknowledge
        RedisServer->>RedisCluster: Set keyslot to new NODE
        RedisCluster->>RedisServer: Acknowledge
    end

    Client->>RedisServer: yield_keyslots(p)
    RedisServer->>RedisCluster: Get shard info
    RedisCluster->>RedisServer: Return shard info
    RedisServer->>Client: Return keyslots to be migrated
```

### Dive inside DiskCache
```mermaid
flowchart TB
    start(Get File) -->|Convert uid to string| lockCache(Lock Cache)
    lockCache -->|Location Lookup| checkRedirect{Is Redirect Needed?}
    checkRedirect -->|Yes| redirect[Redirect to S3 URL]
    checkRedirect -->|No| checkCache{Is File in Cache?}
    checkCache -->|Yes| fileInCache[Use Cached File]
    checkCache -->|No| fetchS3[Fetch File from S3]
    fetchS3 -->|Fetch Success| setCacheLoc[Set File Cache Location]
    fetchS3 -->|Fetch Fail| notFound[Return 'Not Found']
    setCacheLoc -->|Set Location Success| updateAccess[Update Access Order]
    updateAccess -->|Update Complete| openFile[Open File from Cache]
    
    %% Detail of fetching file from S3 and ensuring capacity
    subgraph fetchFromS3[ ]
        fetchS3 -->|Fetch File| ensureCap[Ensure Capacity]
        ensureCap -->|Ensure Complete| createFile[Create File in Cache]
        createFile -->|File Created| setSize[Set File Size]
        setSize --> setCacheLoc
    end

    %% Detail of ensuring capacity
    subgraph ensureCapacity[ ]
        ensureCap -->|Check Capacity| evictionCheck{Is Eviction Needed?}
        evictionCheck -->|No| createFile
        evictionCheck -->|Yes| evictFile[Evict Least Used File]
        evictFile --> removeMeta[Remove File From Metadata]
        removeMeta -->|Continue Checking| evictionCheck
    end

    %% Detail of updating access order
    subgraph updateOrder[ ]
        updateAccess --> removeOld[Remove Old Access Record]
        removeOld --> addNew[Add New Access Record]
        addNew --> openFile
    end
```

### Metadata Migration
```mermaid
flowchart TD
    A[Begin Key Migration Process] --> B[Determine Key Slots to Migrate Using yield_keyslots]
    B --> C[Identify Target Nodes for Migration]
    C --> D{For Each Key Slot}
    D --> E[Use migrate_keyslot_to to Migrate Key Slot to Target Node]
    E --> F[Target Node Receives Key Slot]
    F --> G[Use import_keyslot on Target Node to Update Ownership]
    G --> H{More Key Slots?}
    H -->|Yes| D
    H -->|No| I[Migration Process Complete]
```