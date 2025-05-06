package com.nimbus;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.nimbus.exceptions.ExternalModificationException;
import com.nimbus.exceptions.LocalLockExistsException;
import com.nimbus.exceptions.RemoteLockExistsException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DistributedLockTest {

    static final String BUCKET = "test-bucket";

    @RegisterExtension
    static final S3MockExtension s3mock = S3MockExtension.builder()
            .withSecureConnection(false)
            .withInitialBuckets(BUCKET)
            .silent()
            .build();

    private S3AsyncClient client;
    private DistributedLock lock;

    @BeforeAll
    void setup() {
        client = S3AsyncClient.builder()
                .endpointOverride(URI.create(s3mock.getServiceEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foo", "bar")))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .build();

        lock = new DistributedLock(client);
    }

    private LockEntry lockEntry(String key) {
        return new LockEntry(BUCKET, key);
    }

    @Test @Timeout(5)
    void basicAcquisitionAndRelease() throws Exception {
        CompletableFuture<Void> future = lock.lock(
                lockEntry("lock-1"),
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                () -> CompletableFuture.completedFuture(null)
        );

        assertDoesNotThrow(() -> future.get());
    }

    @Test @Timeout(10)
    void heartbeatKeepsLockAlive() throws Exception {
        CompletableFuture<Void> work = new CompletableFuture<>();

        CompletableFuture<Void> result = lock.lock(
                lockEntry("lock-2"),
                Duration.ofSeconds(1),
                Duration.ofSeconds(5),
                () -> work
        );

        Thread.sleep(4000); // Give heartbeat time to run
        work.complete(null);

        assertDoesNotThrow(() -> result.get());
    }

    @Test @Timeout(6)
    void timeoutTriggersWhenWorkDoesNotFinish() {
        CompletableFuture<Void> future = lock.lock(
                lockEntry("lock-3"),
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                () -> new CompletableFuture<>()
        );

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(TimeoutException.class, ex.getCause());
    }

    @Test @Timeout(10)
    @Disabled("S3Mock doesnt appear to support conditional writes properly yet")
    void externalModificationTriggersFailure() throws Exception {
        LockEntry entry = lockEntry("lock-4");

        CompletableFuture<Void> result = lock.lock(
                entry,
                Duration.ofSeconds(1),
                Duration.ofSeconds(8),
                () -> {
                    try {
                        PutObjectRequest putReq = PutObjectRequest.builder()
                                .bucket(entry.bucket())
                                .key(entry.fileName())
                                .build();

                        Thread.sleep(1500);
                        client.putObject(putReq, AsyncRequestBody.fromBytes("deadbeef".getBytes(StandardCharsets.UTF_8)))
                                .whenComplete((res, ex) -> {
                           if (ex != null)
                               System.out.println(ex.getMessage());
                           else
                               System.out.println(res);
                        }).get();

                        Thread.sleep(3000);

                        return CompletableFuture.completedFuture(null);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        ExecutionException ex = assertThrows(ExecutionException.class, result::get);
        assertInstanceOf(ExternalModificationException.class, ex.getCause());
    }

    @Test @Timeout(5)
    void noExistingFileAllowsLock() throws Exception {
        CompletableFuture<Void> result = lock.lock(
                lockEntry("lock-5"),
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                () -> CompletableFuture.completedFuture(null)
        );

        assertDoesNotThrow(() -> result.get());
    }

    @Test @Timeout(5)
    void existingUnexpiredFileCausesRemoteLockError() throws Exception {
        String key = "lock-6";

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(BUCKET)
                .key(lockEntry(key).fileName())
                .expires(java.time.Instant.now().plusSeconds(60))
                .build();

        client.putObject(req, AsyncRequestBody.empty()).get();

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
                lock.lock(lockEntry(key), Duration.ofSeconds(1), Duration.ofSeconds(3),
                        () -> CompletableFuture.completedFuture(null)).get()
        );

        assertInstanceOf(RemoteLockExistsException.class, ex.getCause());
    }

    @Test @Timeout(5)
    void secondAcquisitionFailsIfHeldLocally() throws Exception {
        LockEntry entry = lockEntry("lock-7");

        CompletableFuture<Void> blocker = new CompletableFuture<>();
        CompletableFuture<Void> first = lock.lock(
                entry,
                Duration.ofSeconds(1),
                Duration.ofSeconds(5),
                () -> blocker
        );

        // Let the first lock definitely establish itself
        Thread.sleep(500);

        CompletableFuture<Void> second = lock.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, second::get);
        assertInstanceOf(LocalLockExistsException.class, ex.getCause());

        blocker.cancel(true); // cleanup
    }

    @Test
    @Timeout(5)
    void lockFileIsDeletedAfterCompletion() throws Exception {
        String key = "lock-delete-check";
        LockEntry entry = lockEntry(key);

        CompletableFuture<Void> result = lock.lock(
                entry,
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                () -> CompletableFuture.completedFuture(null)
        );

        assertDoesNotThrow(() -> result.get());

        CompletableFuture<Boolean> exists = client.headObject(r -> r
                        .bucket(BUCKET)
                        .key(entry.fileName()))
                .thenApply(head -> {
                    System.out.println("exists: " + head);
                    return true;
                })
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof NoSuchKeyException ||
                            ex.getCause() instanceof S3Exception s3e && s3e.statusCode() == 404) {
                        return false;
                    }
                    throw new CompletionException(ex);
                });

        assertFalse(exists.get(), "Expected lock file to be deleted after release");
    }


}
