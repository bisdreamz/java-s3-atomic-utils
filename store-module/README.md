# S3 Atomic Store

A flexible, ETag-based atomic object store using Amazon S3. Designed for distributed coordination without requiring external databases like DynamoDB.

## Features

- 🔄 Safe concurrent writes via ETag validation
- 🧠 Simple interface for reading, updating, and deleting S3 objects
- 🧩 Works with any serializable format (POJO, byte[], or custom)
- ✅ Supports testing with [Adobe S3Mock](https://github.com/adobe/S3Mock)

---

## Add to Your Project

```xml
<dependency>
  <groupId>com.nimbus</groupId>
  <artifactId>s3-atomic-store</artifactId>
  <version>1.0.0</version>
</dependency>
```

---

## Quick Start

### Byte Array Store

```java
AtomicS3Store<byte[]> store = new AtomicS3Store<>(
    s3Client,
    new ByteArraySerializer()
);

// Save binary data
store.put(ObjectState.of(null, "my-bucket", "foo.bin", new byte[]{1, 2, 3}));

// Retrieve
ObjectState<byte[]> result = store.get("my-bucket", "foo.bin").join();
System.out.println(Arrays.toString(result.data()));
```

### POJO Store

```java
AtomicS3Store<MyPojo> store = new AtomicS3Store<>(
    s3Client,
    new JacksonSerializer<>(MyPojo.class) // or new JacksonSerializer(pojo.class, yourJacksonMapper)
);

// Save a POJO
MyPojo pojo = new MyPojo("example", 42);
store.put(ObjectState.of(null, "my-bucket", "pojo.json", pojo)).join();

// Update with ETag validation
ObjectState<MyPojo> current = store.get("my-bucket", "pojo.json").join();
MyPojo updated = new MyPojo("example", 43);
store.put(ObjectState.of(current.etag(), current.bucket(), current.key(), updated));
```

---

## Conflict Detection

All writes require an ETag match unless you're creating a new object. If the stored object was changed by another process, a `ConcurrentModificationException` will be thrown.

---

## Custom Serialization

You can define your own encoding by implementing the interface:

```java
public interface DataSerializer<T> {
    byte[] serialize(T obj) throws IOException;
    T deserialize(byte[] bytes) throws IOException;
}
```

```java
AtomicS3Store<MyCustomFormat> store = new AtomicS3Store<>(
    s3Client,
    new MyCustomSerializer()
);
```

---

## Testing with S3Mock

```java
S3MockApplication server = DistributedLock.getLocalMockServer("/tmp/s3-data", false);
S3AsyncClient mockClient = DistributedLock.getLocalMockClient();

AtomicS3Store<byte[]> store = new AtomicS3Store<>(mockClient, new ByteArraySerializer());
```

---

## License

MIT © 2025 Evan
