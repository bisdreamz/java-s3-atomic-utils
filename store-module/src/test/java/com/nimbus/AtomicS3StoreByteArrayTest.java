package com.nimbus;

import com.adobe.testing.s3mock.S3MockApplication;
import com.nimbus.serializers.ByteArraySerializer;
import com.nimbus.serializers.DataSerializer;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AtomicS3StoreByteArrayTest {

    private static final String BUCKET = "byte-test-bucket";

    private S3MockApplication mockServer;
    private S3AsyncClient client;
    private AtomicS3Store<byte[]> byteStore;
    private Path tempDir;

    @BeforeAll
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("s3mock-test-array");

        mockServer = S3MockApplication.start(new HashMap<>(Map.of(
                S3MockApplication.PROP_ROOT_DIRECTORY, tempDir.toString(),
                S3MockApplication.PROP_HTTP_PORT, "9090",
                S3MockApplication.PROP_SECURE_CONNECTION, false,
                S3MockApplication.PROP_INITIAL_BUCKETS, BUCKET
        )));

        Thread.sleep(500);

        client = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:9090"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foo", "bar")))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .build();

        byteStore = new AtomicS3Store<>(client, new ByteArraySerializer());
    }

    @AfterAll
    void teardown() throws Exception {
        if (mockServer != null) {
            mockServer.stop();
        }
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test @Timeout(5)
    void getMissingReturnsNull() {
        CompletableFuture<ObjectState<byte[]>> f = byteStore.get(BUCKET, "missing-key");
        assertNull(f.join(), "Missing key should yield null");
    }

    @Test @Timeout(5)
    void existsMissingThrowsNoSuchKey() {
        CompletableFuture<ObjectState<byte[]>> f = byteStore.exists(BUCKET, "no-such");
        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        assertTrue(ex.getCause() instanceof NoSuchKeyException);
    }

    @Test @Timeout(5)
    void putAndGetRoundTrip() throws Exception {
        String key = "foo.bin";
        byte[] payload = new byte[]{1,2,3,4};

        // Initial put with null etag
        ObjectState<byte[]> initial = ObjectState.of(null, BUCKET, key, payload);
        ObjectState<byte[]> stored = byteStore.put(initial).get();
        assertNotNull(stored.etag(), "ETag should be assigned");

        // Head check
        ObjectState<byte[]> head = byteStore.exists(BUCKET, key).join();
        assertEquals(stored.etag(), head.etag());
        assertNull(head.data(), "exists() must return null data");

        // Full get round‑trip
        ObjectState<byte[]> fetched = byteStore.get(BUCKET, key).get();
        assertEquals(stored.etag(), fetched.etag());
        assertArrayEquals(payload, fetched.data(), "Payload must match");
    }

    @Test @Timeout(5)
    @Disabled("S3Mock does not yet support conditional write testing")
    void conditionalUpdateAndConflict() throws Exception {
        String key = "bar.bin";
        byte[] a = new byte[]{9};
        byte[] b = new byte[]{7};

        ObjectState<byte[]> s1 = byteStore.put(ObjectState.of(null, BUCKET, key, a)).get();
        ObjectState<byte[]> s2 = byteStore.put(ObjectState.of(s1.etag(), BUCKET, key, b)).get();
        assertNotEquals(s1.etag(), s2.etag());

        CompletableFuture<ObjectState<byte[]>> conflict =
                byteStore.put(ObjectState.of(s1.etag(), BUCKET, key, new byte[]{0}));
        ExecutionException ex = assertThrows(ExecutionException.class, conflict::get);
        assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }

    @Test @Timeout(5)
    @Disabled("S3Mock does not yet support conditional write testing")
    void deleteAndConditionalDelete() throws Exception {
        String key = "del.bin";
        byte[] data = new byte[]{5,5};

        ObjectState<byte[]> s = byteStore.put(ObjectState.of(null, BUCKET, key, data)).get();
        byteStore.delete(s).join();
        assertNull(byteStore.get(BUCKET, key).join());

        s = byteStore.put(ObjectState.of(null, BUCKET, key, data)).get();
        CompletableFuture<Void> bad = byteStore.delete(
                ObjectState.of("wrong-etag", BUCKET, key, null)
        );
        ExecutionException ex2 = assertThrows(ExecutionException.class, bad::get);
        assertTrue(ex2.getCause() instanceof ConcurrentModificationException);
    }

    // ─── Serialization‐failure tests ───────────────────────────────────────────

    private static class FailingSerializer implements DataSerializer<byte[]> {
        @Override
        public byte[] serialize(byte[] obj) {
            throw new RuntimeException("serialize-failure");
        }
        @Override
        public byte[] deserialize(byte[] bytes) {
            throw new RuntimeException("deserialize-failure");
        }
    }

    @Test @Timeout(5)
    void putWithSerializerFailureThrowsIllegalState() {
        AtomicS3Store<byte[]> badStore =
                new AtomicS3Store<>(client, new FailingSerializer());
        ObjectState<byte[]> state = ObjectState.of(null, BUCKET, "x", new byte[]{0});
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> badStore.put(state)
        );
        assertTrue(ex.getMessage().contains("serialize-failure"));
    }

    @Test @Timeout(5)
    void getWithDeserializerFailureThrowsExecutionException() throws Exception {
        String key = "to-fail.bin";
        byte[] foo = new byte[]{1,2,3};
        ObjectState<byte[]> saved = byteStore.put(ObjectState.of(null, BUCKET, key, foo)).get();

        AtomicS3Store<byte[]> badStore =
                new AtomicS3Store<>(client, new FailingSerializer());
        CompletableFuture<ObjectState<byte[]>> f = badStore.get(BUCKET, key);

        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof IllegalStateException);
        assertTrue(cause.getMessage().contains("deserialize-failure"));
    }

    // ─── S3‐error propagation tests ────────────────────────────────────────────

    @Test @Timeout(5)
    void existsOnUnknownBucketPropagatesS3Exception() {
        CompletableFuture<ObjectState<byte[]>> f = byteStore.exists("no-such-bucket", "any");
        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        assertTrue(ex.getCause() instanceof S3Exception);
    }

    @Test @Timeout(5)
    void getOnUnknownBucketPropagatesS3Exception() {
        CompletableFuture<ObjectState<byte[]>> f = byteStore.get("no-such-bucket", "any");
        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        assertTrue(ex.getCause() instanceof S3Exception);
    }

    // ─── delete on missing key is no‐op ────────────────────────────────────────

    @Test @Timeout(5)
    void deleteNonexistentKeyCompletesNormally() {
        ObjectState<byte[]> stubState =
                ObjectState.of(null, BUCKET, "never.bin", null);
        assertDoesNotThrow(() -> byteStore.delete(stubState).join());
        assertNull(byteStore.get(BUCKET, "never.bin").join());
    }
}
