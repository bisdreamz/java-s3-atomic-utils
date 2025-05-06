package com.nimbus;

import com.adobe.testing.s3mock.S3MockApplication;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the convenience methods provided by DistributedLock, which provides a pre configured local fs server/client
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DistributedLockMockTest {

    private static final String MOCK_BUCKET = "default-mock-bucket";

    private S3MockApplication mockServer;
    private DistributedLock lock;
    private S3AsyncClient mockClient;

    private Path dataDir;

    @BeforeAll
    void startMockServer() throws Exception {
        dataDir = Files.createTempDirectory("s3mock-test");

        if (!Files.exists(dataDir))
            dataDir = Files.createDirectory(dataDir);

        mockServer = DistributedLock.getLocalMockServer(dataDir.toAbsolutePath().toString(), false);

        Thread.sleep(1000);

        mockClient = DistributedLock.getLocalMockClient();
        lock = new DistributedLock(mockClient);
    }

    @AfterAll
    void stopMockServer() throws Exception {
        if (mockServer != null)
            mockServer.stop();

        if (dataDir != null) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    private void assertObjectExists(String bucket, String key) throws Exception {
        HeadObjectResponse head = mockClient.headObject(
                HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build()
        ).get();

        assertNotNull(head);
        assertTrue(head.contentLength() > 0, "Expected object to contain data");
    }

    @Test
    @Timeout(10)
    void testAcquireAndReleaseLock() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-acquire-lock");

        CompletableFuture<Void> fut = lock.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> {
                    try {
                        assertObjectExists(entry.bucket(), entry.fileName());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return CompletableFuture.completedFuture(null);
                }
        );

        fut.get();
    }


    @Test
    @Timeout(10)
    void testFailsIfLockAlreadyHeldLocally() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-lock");

        CompletableFuture<Void> fut1 = lock.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> {
                    CompletableFuture<Void> fut2 = lock.lock(
                            entry,
                            Duration.ofSeconds(2),
                            Duration.ofSeconds(5),
                            () -> CompletableFuture.completedFuture(null)
                    );

                    ExecutionException ex = assertThrows(ExecutionException.class, fut2::get);
                    assertTrue(ex.getCause() instanceof com.nimbus.exceptions.LocalLockExistsException);
                    return CompletableFuture.completedFuture(null);
                }
        );

        fut1.get();
    }

    @Test
    @Timeout(10)
    void testLockFileOverwrittenWithHeartbeats() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "heartbeat-lock");

        CompletableFuture<Void> fut = lock.lock(
                entry,
                Duration.ofSeconds(1),
                Duration.ofSeconds(4),
                () -> {
                    try {
                        Thread.sleep(3000);
                        assertObjectExists(entry.bucket(), entry.fileName());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return CompletableFuture.completedFuture(null);
                }
        );

        fut.get();
    }

}
