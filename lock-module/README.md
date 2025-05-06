# S3 Distributed Lock

A lightweight distributed locking library using Amazon S3 as the coordination mechanism — no external database required.

This module allows safe mutual exclusion across processes or services using S3 object metadata and ETags, enabling distributed locking without the complexity of managing other coordination systems.

---

## Features

- 🔐 Safe mutual exclusion via atomic ETag writes
- ⏱️ Lock expiration with configurable timeouts
- 🔄 Automatic heartbeat renewal to extend lock
- 🧠 Local JVM conflict detection
- ✅ Built-in support for local testing with [Adobe S3Mock](https://github.com/adobe/S3Mock)
- 🧰 Simple usage and no dependency on external coordination services

---

## Add to Your Project

```xml
<dependency>
  <groupId>com.nimbus</groupId>
  <artifactId>s3-distributed-lock</artifactId>
  <version>1.0.0</version>
</dependency>
```

---

## Quick Start

```java
S3AsyncClient s3Client = S3AsyncClient.create();
DistributedLock lock = new DistributedLock(s3Client);

lock.lock(
    new LockEntry("my-bucket", "lockName"),
    Duration.ofSeconds(10),   // Heartbeat interval
    Duration.ofMinutes(5),    // Max lock duration
    () -> {
        // Perform protected work
        return performWork().thenApply(result -> null);
    }
);
```

---

## Behavior

- If the lock file does not exist, it is created.
- The ETag is used as a version check during each heartbeat and renewal.
- If the lock already exists, the behavior depends on:
  - Whether the local JVM already holds the lock
  - Whether the lock has expired
  - Whether an external process modified the lock file unexpectedly

---

## 🫀 How Heartbeats Work

The lock module uses a heartbeat interval to determine when a lock is considered expired if the owner becomes unresponsive (e.g., JVM crash, power loss).

- When a lock is acquired, a small file is written to S3 with an expiration timestamp.
- While the lock is held, the application **automatically rewrites the file periodically** (the "heartbeat") to update this timestamp.
- If the application crashes or becomes unreachable, no further heartbeats are sent.

⏱️ **Heartbeat Interval = Expiration Sensitivity**

- If your heartbeat interval is `10 seconds`, then another process can safely attempt to acquire the lock **after ~10 seconds of no updates**.
- This is **not** the same as the `maxLockDuration`, which is the longest time a process is allowed to hold the lock (regardless of heartbeat), to safeguard against stale body logic holding the lock indefinitely.

> Think of the heartbeat interval as your *failover delay* — the time another app waits before safely taking over if the lock holder vanishes.

---


## Local Testing

You can use Adobe's S3Mock for running integration tests without AWS:

```java
S3MockApplication server = DistributedLock.getLocalMockServer("/tmp/s3mock", false);
S3AsyncClient mockClient = DistributedLock.getLocalMockClient();

DistributedLock lock = new DistributedLock(mockClient);
```

---

## Advanced Notes

- Each call to `lock()` writes an object with metadata and periodically updates it using `PutObject` with `If-Match`.
- If the `If-Match` condition fails, the lock is considered lost or stolen.
- If the lock holder crashes or stops heartbeating, the lock eventually expires and becomes available for others.

---

## License

MIT © 2025 Evan
