package com.nimbus;

import com.adobe.testing.s3mock.S3MockApplication;
import com.nimbus.serializers.JacksonSerializer;
import com.nimbus.serializers.DataSerializer;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.net.URI;
import java.nio.file.*;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AtomicS3StorePojoTest {

    record TestPojo(String name, int value) {}

    private static final String BUCKET = "test-bucket-pojo";

    private S3MockApplication mockServer;
    private S3AsyncClient client;
    private AtomicS3Store<TestPojo> pojoStore;
    private Path tempDir;

    @BeforeAll
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("s3mock-pojo");
        mockServer = S3MockApplication.start(new HashMap<>(Map.of(
                S3MockApplication.PROP_ROOT_DIRECTORY,    tempDir.toString(),
                S3MockApplication.PROP_HTTP_PORT,         "9090",
                S3MockApplication.PROP_SECURE_CONNECTION, false,
                S3MockApplication.PROP_INITIAL_BUCKETS,   BUCKET
        )));

        Thread.sleep(500);

        client = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:9090"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foo", "bar")))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .build();

        pojoStore = new AtomicS3Store<>(
                client,
                new JacksonSerializer<>(TestPojo.class)
        );
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
        CompletableFuture<ObjectState<TestPojo>> f = pojoStore.get(BUCKET, "missing.json");
        ObjectState<TestPojo> result = f.join();
        assertNull(result, "get(…) on a missing key should return null");
    }

    @Test @Timeout(5)
    void existsMissingThrowsNoSuchKey() {
        CompletableFuture<ObjectState<TestPojo>> f = pojoStore.exists(BUCKET, "nope.json");
        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        assertTrue(ex.getCause() instanceof NoSuchKeyException);
    }

    @Test @Timeout(5)
    void putAndGetPojoRoundTrip() throws Exception {
        String key = "config.json";
        TestPojo original = new TestPojo("alice", 42);

        // PUT initial
        ObjectState<TestPojo> putState =
                pojoStore.put(ObjectState.of(null, BUCKET, key, original))
                        .get();
        assertNotNull(putState.etag(), "ETag must be assigned on first put");

        // GET back
        ObjectState<TestPojo> fetched =
                pojoStore.get(BUCKET, key).get();
        assertEquals(putState.etag(), fetched.etag());
        assertEquals(original, fetched.data(),
                "Round‑trip POJO must match original");

        // EXISTS only etag
        ObjectState<TestPojo> head = pojoStore.exists(BUCKET, key).get();
        assertEquals(putState.etag(), head.etag());
        assertNull(head.data(), "exists() must return null data");
    }

    @Test @Timeout(5)
    @Disabled("S3Mock does not yet support enforcing conditional writes")
    void conditionalUpdateConflict() throws Exception {
        String key = "conflict.json";
        TestPojo first = new TestPojo("bob", 7);
        TestPojo second = new TestPojo("carol", 8);

        ObjectState<TestPojo> s1 =
                pojoStore.put(ObjectState.of(null, BUCKET, key, first)).get();
        ObjectState<TestPojo> s2 =
                pojoStore.put(ObjectState.of(s1.etag(), BUCKET, key, second)).get();

        // stale update fails
        CompletableFuture<ObjectState<TestPojo>> stale =
                pojoStore.put(ObjectState.of(s1.etag(), BUCKET, key, first));
        ExecutionException ex = assertThrows(ExecutionException.class, stale::get);
        assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }

    // ─── Serialization‐failure tests ───────────────────────────────────────────

    private static class FailingPojoSerializer implements DataSerializer<TestPojo> {
        @Override
        public byte[] serialize(TestPojo obj) {
            throw new RuntimeException("pojo‐serialize‐fail");
        }
        @Override
        public TestPojo deserialize(byte[] bytes) {
            throw new RuntimeException("pojo‐deserialize‐fail");
        }
    }

    @Test @Timeout(5)
    void putWithPojoSerializerFailureThrowsIllegalState() {
        AtomicS3Store<TestPojo> badStore =
                new AtomicS3Store<>(client, new FailingPojoSerializer());
        ObjectState<TestPojo> state =
                ObjectState.of(null, BUCKET, "x.json", new TestPojo("x",1));
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> badStore.put(state)
        );
        assertTrue(ex.getMessage().contains("pojo‐serialize‐fail"));
    }

    @Test @Timeout(5)
    void getWithPojoDeserializerFailureThrowsExecutionException() throws Exception {
        String key = "bad.json";
        TestPojo ok = new TestPojo("ok", 1);
        // store good first
        ObjectState<TestPojo> s =
                pojoStore.put(ObjectState.of(null, BUCKET, key, ok)).get();

        AtomicS3Store<TestPojo> badStore =
                new AtomicS3Store<>(client, new FailingPojoSerializer());
        CompletableFuture<ObjectState<TestPojo>> f = badStore.get(BUCKET, key);

        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof IllegalStateException);
        assertTrue(cause.getMessage().contains("pojo‐deserialize‐fail"));
    }

    // ─── S3‐error propagation tests ────────────────────────────────────────────

    @Test @Timeout(5)
    void existsOnUnknownBucketPropagatesS3Exception() {
        CompletableFuture<ObjectState<TestPojo>> f =
                pojoStore.exists("no-such-bucket", "any.json");
        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        assertTrue(ex.getCause() instanceof S3Exception);
    }

    @Test @Timeout(5)
    void getOnUnknownBucketPropagatesS3Exception() {
        CompletableFuture<ObjectState<TestPojo>> f =
                pojoStore.get("no-such-bucket", "any.json");
        ExecutionException ex = assertThrows(ExecutionException.class, f::get);
        assertTrue(ex.getCause() instanceof S3Exception);
    }

    // ─── delete on missing key is no‐op ────────────────────────────────────────

    @Test @Timeout(5)
    void deleteNonexistentPojoKeyCompletesNormally() {
        ObjectState<TestPojo> stubState =
                ObjectState.of(null, BUCKET, "never.json", null);
        assertDoesNotThrow(() -> pojoStore.delete(stubState).join());
        assertNull(pojoStore.get(BUCKET, "never.json").join());
    }
}
