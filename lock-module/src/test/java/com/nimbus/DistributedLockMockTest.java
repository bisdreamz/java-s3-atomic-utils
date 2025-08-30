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
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

        mockServer = DistributedLock.getLocalMockServer(MOCK_BUCKET, dataDir.toAbsolutePath().toString(), false);

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

    @Test
    @Timeout(10)
    void testLockExpiresAfterIntervalWithoutHeartbeat() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-expiry-lock");
        
        // First acquire the lock with a short expiry interval
        CompletableFuture<Void> lockFut = new CompletableFuture<>();
        CompletableFuture<Void> fut = lock.lock(
                entry,
                Duration.ofSeconds(2),  // expiry interval
                Duration.ofSeconds(10), // max duration
                () -> lockFut
        );
        
        // Wait a bit to ensure lock is established
        Thread.sleep(500);
        assertObjectExists(entry.bucket(), entry.fileName());
        
        // Simulate crash by completing the future (stops heartbeats)
        lockFut.complete(null);
        fut.get();
        
        // Wait for expiry interval to pass
        Thread.sleep(2500);
        
        // Now another client should be able to acquire the lock
        DistributedLock anotherClient = new DistributedLock(mockClient);
        CompletableFuture<Void> fut2 = anotherClient.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> CompletableFuture.completedFuture(null)
        );
        
        // This should succeed since the lock expired
        fut2.get();
    }

    @Test
    @Timeout(10)
    void testLockNotAvailableBeforeExpiry() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-timing-lock");
        
        // First acquire the lock - keep it held
        CompletableFuture<Void> lockFut = new CompletableFuture<>();
        lock.lock(
                entry,
                Duration.ofSeconds(3),  // expiry interval
                Duration.ofSeconds(10), // max duration
                () -> lockFut
        );
        
        // Wait to ensure lock is established
        Thread.sleep(500);
        
        // Try to acquire from another client - should fail (lock not expired)
        DistributedLock anotherClient = new DistributedLock(mockClient);
        CompletableFuture<Void> fut2 = anotherClient.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> CompletableFuture.completedFuture(null)
        );
        
        ExecutionException ex = assertThrows(ExecutionException.class, () -> fut2.get(1, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof com.nimbus.exceptions.RemoteLockExistsException);
        
        // Complete the first lock to clean up
        lockFut.complete(null);
    }

    @Test
    @Timeout(15)
    void testLockExpiresAfterCrashWithoutDelete() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-crash-lock");
        
        // First, manually create a lock file that will expire
        Instant expiry = Instant.now().plusSeconds(2);
        mockClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(entry.bucket())
                        .key(entry.fileName())
                        .expires(expiry)
                        .build(),
                software.amazon.awssdk.core.async.AsyncRequestBody.fromString("lock-data")
        ).get();
        
        // Try to acquire immediately - should fail (not expired)
        DistributedLock client = new DistributedLock(mockClient);
        CompletableFuture<Void> fut1 = client.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> CompletableFuture.completedFuture(null)
        );
        
        ExecutionException ex = assertThrows(ExecutionException.class, () -> fut1.get(1, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof com.nimbus.exceptions.RemoteLockExistsException);
        
        // Wait for expiry
        Thread.sleep(2500);
        
        // Now should be able to acquire (lock expired)
        CompletableFuture<Void> fut2 = client.lock(
                entry,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                () -> CompletableFuture.completedFuture(null)
        );
        
        fut2.get(); // Should succeed
    }

    @Test
    @Timeout(10)
    void testInitialLockHasCorrectExpiry() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-initial-expiry");
        
        Instant beforeLock = Instant.now();
        
        CompletableFuture<Void> lockFut = new CompletableFuture<>();
        CompletableFuture<Void> fut = lock.lock(
                entry,
                Duration.ofSeconds(2),  // expiry interval - should be used for expiry
                Duration.ofSeconds(10), // max duration - should NOT be used for expiry
                () -> {
                    try {
                        // Check the expiry of the lock file
                        HeadObjectResponse head = mockClient.headObject(
                                HeadObjectRequest.builder()
                                        .bucket(entry.bucket())
                                        .key(entry.fileName())
                                        .build()
                        ).get();
                        
                        Instant expires = head.expires();
                        assertNotNull(expires, "Lock should have an expiry time");
                        
                        // Expiry should be approximately 2 seconds from when we acquired the lock
                        long secondsUntilExpiry = expires.getEpochSecond() - beforeLock.getEpochSecond();
                        assertTrue(secondsUntilExpiry >= 1 && secondsUntilExpiry <= 3,
                                "Initial lock expiry should be ~2 seconds, not ~10 seconds. Got: " + secondsUntilExpiry);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return lockFut;
                }
        );
        
        // Let the test run briefly then complete
        Thread.sleep(500);
        lockFut.complete(null);
        fut.get();
    }

    @Test
    @Timeout(10)
    void testHeartbeatMaintainsCorrectExpiry() throws Exception {
        LockEntry entry = new LockEntry(MOCK_BUCKET, "test-heartbeat-expiry");
        
        CompletableFuture<Void> lockFut = new CompletableFuture<>();
        CompletableFuture<Void> fut = lock.lock(
                entry,
                Duration.ofSeconds(2),  // expiry interval
                Duration.ofSeconds(10), // max duration
                () -> {
                    try {
                        // Wait for at least one heartbeat to occur
                        Thread.sleep(1700); // Just past the safety margin scheduling time
                        
                        // Check the expiry after heartbeat
                        HeadObjectResponse head = mockClient.headObject(
                                HeadObjectRequest.builder()
                                        .bucket(entry.bucket())
                                        .key(entry.fileName())
                                        .build()
                        ).get();
                        
                        Instant expires = head.expires();
                        assertNotNull(expires, "Lock should have an expiry time after heartbeat");
                        
                        // Expiry should still be approximately 2 seconds from now
                        long secondsUntilExpiry = expires.getEpochSecond() - Instant.now().getEpochSecond();
                        assertTrue(secondsUntilExpiry >= 1 && secondsUntilExpiry <= 3,
                                "After heartbeat, lock expiry should still be ~2 seconds from now. Got: " + secondsUntilExpiry);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return lockFut;
                }
        );
        
        // Let it run a bit more then complete
        Thread.sleep(2000);
        lockFut.complete(null);
        fut.get();
    }

}
