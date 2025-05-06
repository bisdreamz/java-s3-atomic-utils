# S3 Atomic Utils

A robust Java library for distributed coordination and atomic operations using Amazon S3 as a backing store.

## Overview

This library provides two key components for distributed applications using S3:

1. **Distributed Locking (`lock-module`)** — Use S3 objects as distributed locks with automatic heartbeat and expiration.
2. **Atomic S3 Store (`store-module`)** — Store and update objects in S3 with strong consistency guarantees.

Both components use S3's ETag mechanism to provide safe concurrent access from multiple application instances.

---

## Modules

### Lock Module (`s3-distributed-lock`)

Provides distributed locking using S3 objects as the coordination mechanism.

**Features:**

- Lock expiration with configurable timeouts
- Automatic heartbeats to maintain lock validity
- Fail-fast local lock re-entry detection
- Safety checks against external modification

### Store Module (`s3-atomic-store`)

Provides atomic updates and retrieval of S3 objects, using ETags to prevent race conditions.

**Features:**

- Optimistic concurrency control via ETags
- POJO and raw byte support out-of-the-box
- Graceful error and conflict handling
- Works seamlessly with AWS S3 or S3Mock for testing

---

## Usage Examples

### Distributed Lock

```java
DistributedLock lock = new DistributedLock(s3Client);

CompletableFuture<Void> result = lock.lock(
    new LockEntry("my-bucket", "process-lock"),
    Duration.ofSeconds(10),   // Heartbeat interval
    Duration.ofMinutes(30),   // Maximum duration
    () -> {
        // Critical section
        return performWorkAsync();
    }
);
```

### Atomic S3 Store

```java
AtomicS3Store<Config> configStore = new AtomicS3Store<>(
    s3Client,
    new JacksonSerializer<>(Config.class)
);

// Fetch existing config
ObjectState<Config> state = configStore.get("config-bucket", "app-config.json").join();
if (state != null) {
    Config cfg = state.data();

    // ...later when you want to update:
    Config updated = new Config(cfg.name(), cfg.value() + 1);

    configStore.put(ObjectState.of(
        state.etag(),
        state.bucket(),
        state.key(),
        updated
    ));
}
```

---

## Serializers

The store module includes:

- `JacksonSerializer<T>` – for structured POJO storage
- `ByteArraySerializer` – for storing raw bytes

You may implement `DataSerializer<T>` for custom formats if needed.

---

## Testing with S3Mock

```java
S3MockApplication mockServer = DistributedLock.getLocalMockServer(
    "/tmp/s3-data",
    false // Don't delete files on exit
);

S3AsyncClient mockClient = DistributedLock.getLocalMockClient();

DistributedLock lock = new DistributedLock(mockClient);
AtomicS3Store<byte[]> store = new AtomicS3Store<>(mockClient, new ByteArraySerializer());
```

---

## Requirements

- Java 17+
- AWS SDK v2
- JUnit 5 (for tests)
- [Adobe S3Mock](https://github.com/adobe/S3Mock) (for local testing)

---

## License

MIT © 2025 Evan
