package com.nimbus;

import com.adobe.testing.s3mock.S3MockApplication;
import com.nimbus.exceptions.ExternalModificationException;
import com.nimbus.exceptions.LocalLockExistsException;
import com.nimbus.exceptions.RemoteLockExistsException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class DistributedLock {

    private static final Duration SAFETY_MARGIN = Duration.ofMillis(500);
    private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor(t -> {
        Thread thread = new Thread(t);

        thread.setDaemon(true);
        thread.setName("distributed-lock-monitor-thread");

        return thread;
    });

    private final S3AsyncClient client;
    private final Map<String, String> etagCache;

    /**
     * Convenience method for starting an apache S3Mock server to enable easy testing
     * with local fs storage.
     * @implNote Runs on port 9090, requires ssl be disabled and clients force path style access
     * @param bucket The initial bucket to create
     * @param dataDir Directory to store data
     * @param keepFilesOnExit Whether to keep file data on exit. False to delete all data upon close.
     * @return {@link S3MockApplication} which should be stopped upon application shutdown
     */
    public static S3MockApplication getLocalMockServer(String bucket, String dataDir, boolean keepFilesOnExit) {
        if (dataDir == null)
            throw new IllegalArgumentException("dataDir must not be null");

        return S3MockApplication.start(new HashMap<>(Map.of(
                S3MockApplication.PROP_ROOT_DIRECTORY, dataDir,
                S3MockApplication.PROP_HTTP_PORT, "9090", // you can make this dynamic if needed
                S3MockApplication.PROP_SECURE_CONNECTION, false,
                S3MockApplication.PROP_INITIAL_BUCKETS, bucket,
                S3MockApplication.PROP_SILENT, true,
                "com.adobe.testing.s3mock.domain.retainFilesOnExit", keepFilesOnExit
        )));
    }

    /**
     * Convenience method for starting an apache S3Mock server to enable easy testing
     * with local fs storage. Uses "default-mock-bucket" as the initial bucket.
     * @implNote Runs on port 9090, requires ssl be disabled and clients force path style access
     * @param dataDir Directory to store data
     * @param keepFilesOnExit Whether to keep file data on exit. False to delete all data upon close.
     * @return {@link S3MockApplication} which should be stopped upon application shutdown
     */
    public static S3MockApplication getLocalMockServer(String dataDir, boolean keepFilesOnExit) {
        return getLocalMockServer("default-mock-bucket", dataDir, keepFilesOnExit);
    }

    /**
     * Convenience method to create a client for accessing the local mock server
     * created by {@link #getLocalMockServer(String, boolean)}
     * @return A {@link S3AsyncClient} preconfigured with bum credentials and pointed to the local mock server
     */
    public static S3AsyncClient getLocalMockClient() {
        return S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:9090"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foo", "bar")))
                .forcePathStyle(true) // required for all mock S3 implementations
                .region(Region.US_EAST_1)
                .build();
    }

    /**
     * Construct a new distributed lock instance
     * @param s3Client A configured {@link software.amazon.awssdk.services.s3.S3Client} instance,
     *                 which should already have auth and all configuration applied.
     */
    public DistributedLock(S3AsyncClient s3Client) {
        if (s3Client == null)
            throw new NullPointerException("s3Client is null");

        this.client = s3Client;
        this.etagCache = new ConcurrentHashMap<>();
    }

    /**
     * Attempts to acquire the initial lock.
     *
     * Acquisition will succeed if no lock currently exists, or if its expiration
     * has passed.
     * @param entry Bucket
     * @param expiry Expiry to assign if we succed in acquisition
     * @return A future which completes with the new lock's etag value
     */
    private CompletableFuture<String> headAndAcquireInitial(LockEntry entry, Duration expiry) {
        HeadObjectRequest headReq = HeadObjectRequest.builder()
                .bucket(entry.bucket())
                .key(entry.fileName())
                .build();

        return client.headObject(headReq)
                .thenCompose(head -> {
                    Instant expiresAt = head.expires();
                    if (expiresAt != null && expiresAt.isAfter(Instant.now())) {
                        return CompletableFuture.failedFuture(
                                new RemoteLockExistsException("Remote lock exists until " + expiresAt));
                    }

                    return putOrUpdateLock(entry.bucket(), entry.fileName(), expiry, head.eTag());
                })
                .exceptionallyCompose(ex -> {
                    if (ex.getCause() instanceof NoSuchKeyException
                            || ex.getCause() instanceof S3Exception s3e && s3e.statusCode() == 404) {
                        return putOrUpdateLock(entry.bucket(), entry.fileName(), expiry,null);
                    }

                    return CompletableFuture.failedFuture(ex.getCause());
                });
    }

    /**
     * Creates or updates (hearbeat) a specified lock file. Will succeed if there is no existing lock file,
     * or if the existing lock file matches the expected etag value. This is used for keeping a lock 'fresh'
     * @param bucket S3 bucket of lock location
     * @param key Key to specific lock file
     * @param expectedEtag If not provided, only put file if no existing lock exists. If etag provided,
     *                     only overwrite lock file if the etag matches expected value, e.g. ensure no one
     *                     has borked our lock
     * @return Future resolving to the updated etag value
     */
    private CompletableFuture<String> putOrUpdateLock(String bucket, String key, Duration expiry, String expectedEtag) {
        PutObjectRequest.Builder putBuilder = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .ifMatch(expectedEtag)
                .expires(Instant.now().plus(expiry));

        PutObjectRequest putReq = putBuilder.build();

        return this.client.putObject(putReq, AsyncRequestBody.fromBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)))
                .thenCompose(res -> {
            if (!res.sdkHttpResponse().isSuccessful()) {
                return CompletableFuture.failedFuture(
                        new IOException("Failed to put lock file! Panic! " + res.sdkHttpResponse().statusText()));
            }

            return CompletableFuture.completedFuture(res.eTag());
        });
    }

    /**
     * Begin the repeating heartbeat tasks to refresh the remote lock instance and keep it live.
     *
     * @param entry The lock entry containing bucket and key information
     * @param interval The interval at which to refresh the lock expiry
     * @param lockFut The future to complete exceptionally if heartbeat fails
     * @return AtomicReference to the scheduled heartbeat task for cancellation
     */
    private AtomicReference<ScheduledFuture<?>> scheduleLockRefresh(
            LockEntry entry,
            Duration interval,
            CompletableFuture<Void> lockFut
    ) {
        if (interval.toSeconds() < 1)
            throw new IllegalArgumentException("Heartbeat interval must be at least 1 second");

        long schedulingIntervalMs = interval.toMillis() - SAFETY_MARGIN.toMillis();
        if (schedulingIntervalMs <= 0)
            throw new IllegalArgumentException("Expiry interval must be greater than safety margin (" + SAFETY_MARGIN.toMillis() + "ms)");

        AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

        class HeartbeatTask implements Runnable {
            @Override
            public void run() {
                String currentEtag = etagCache.get(entry.absUri());

                putOrUpdateLock(entry.bucket(), entry.fileName(), interval, currentEtag)
                        .whenComplete((newEtag, ex) -> {
                            if (ex != null) {
                                if (ex.getCause() instanceof S3Exception s3e) {
                                    int status = s3e.statusCode();

                                    // if our lock file is gone or etag mismatch, someone borked us man!
                                    if (status == 404 || status == 412) {
                                        lockFut.completeExceptionally(
                                                new ExternalModificationException(
                                                        "Lock file was externally modified or deleted (HTTP " + status + ")",
                                                        s3e)
                                        );

                                        return;
                                    }
                                }

                                lockFut.completeExceptionally(ex);
                            } else {
                                etagCache.put(entry.absUri(), newEtag);
                                
                                // Self-schedule the next heartbeat if lock is still active
                                if (!lockFut.isDone()) {
                                    try {
                                        futureRef.set(EXECUTOR.schedule(this, schedulingIntervalMs, TimeUnit.MILLISECONDS));
                                    } catch (Exception schedEx) {
                                        lockFut.completeExceptionally(new RuntimeException("Failed to schedule heartbeat", schedEx));
                                    }
                                }
                            }
                        });
            }
        }
        
        HeartbeatTask runner = new HeartbeatTask();

        // Schedule the first heartbeat
        futureRef.set(EXECUTOR.schedule(runner, schedulingIntervalMs, TimeUnit.MILLISECONDS));
        return futureRef;
    }

    /**
     * Attempts to acquire a distributed lock, run the supplied asynchronous work while
     * holding the lock, and then release *only the lock this JVM acquired* when the work completes.
     *
     * <p>If the lock is already held locally, this method fails fast. Once acquired, a background
     * heartbeat will refresh the lock every {@code expiryInterval}—which also defines how long
     * other nodes must wait after a crash or missed heartbeat before they can acquire the lock.
     * Separately, a watchdog enforces a hard {@code maxLockDuration}, the total time this JVM
     * will hold the lock, to guard against hung or never‑returning work.</p>
     *
     * @param entry            the {@link LockEntry} describing bucket & key for the lock
     * @param expiryInterval   how often to refresh this JVM’s lock; also the expiry period
     *                         after which another node may acquire if no heartbeat occurs
     * @param maxLockDuration  the maximum total time this JVM may hold the lock—regardless
     *                         of heartbeats—used as a safety timeout if the work never completes
     * @param lockBody         a supplier of the work to perform while holding the lock;
     *                         returns a future that must complete when the work is done
     * @return a future that completes normally when {@code lockBody} completes, or
     *         exceptionally if acquisition fails, the work throws, heartbeats fail,
     *         or the {@code maxLockDuration} is exceeded
     *
     * @implNote This method only ever deletes/releases the lock file if *this JVM successfully*
     *           acquired it. If the process crashes or network errors prevent release, other nodes
     *           will see the lock expire after {@code expiryInterval} and may then acquire it.
     *
     * @throws IllegalArgumentException   if {@code expiryInterval} is less than 5 seconds
     * @throws LocalLockExistsException   if this JVM already holds the lock locally
     * @throws RemoteLockExistsException  if another node holds an unexpired lock
     * @throws TimeoutException           if the work (and lock) exceeds {@code maxLockDuration}
     */
    public CompletableFuture<Void> lock(
            LockEntry entry,
            Duration expiryInterval,
            Duration maxLockDuration,
            Supplier<CompletableFuture<Void>> lockBody
    ) {
        if (entry == null)
            throw new NullPointerException("entry cannot be null");
        if (expiryInterval == null)
            throw new NullPointerException("expiryInterval cannot be null");
        if (maxLockDuration == null)
            throw new NullPointerException("maxLockDuration cannot be null");
        if (lockBody == null)
            throw new NullPointerException("lockBody cannot be null");
        if (expiryInterval.compareTo(maxLockDuration) >= 0)
            throw new IllegalArgumentException("expiryInterval must be less than maxLockDuration");
        
        if (etagCache.containsKey(entry.absUri()))
            return CompletableFuture.failedFuture(new LocalLockExistsException("We already locally own lock " + entry.absUri()));

        CompletableFuture<Void> lockFut = new CompletableFuture<>();

        headAndAcquireInitial(entry, expiryInterval)
                .thenCompose(etag -> {
                    etagCache.put(entry.absUri(), etag);

                    AtomicReference<ScheduledFuture<?>> hbRef = scheduleLockRefresh(entry, expiryInterval, lockFut);
                    ScheduledFuture<?> watchdog = EXECUTOR.schedule(() -> {
                        lockFut.completeExceptionally(new TimeoutException("Lock " + entry.absUri() + " held longer than " + maxLockDuration));
                    }, maxLockDuration.toMillis(), TimeUnit.MILLISECONDS);

                    // Ensure cleanup happens regardless of how lockFut completes
                    lockFut.whenComplete((__, ___) -> {
                        // Cancel heartbeat - use loop to handle race condition
                        ScheduledFuture<?> hb;
                        while ((hb = hbRef.getAndSet(null)) != null) {
                            hb.cancel(true);
                        }
                        watchdog.cancel(true);
                    });

                    return lockBody.get();
                })
                .handle((res, ex) -> {
                    etagCache.remove(entry.absUri());

                    if (ex != null) {
                        lockFut.completeExceptionally(ex);
                    } else {
                        client.deleteObject(DeleteObjectRequest.builder()
                                .bucket(entry.bucket())
                                .key(entry.fileName())
                                .build()).whenComplete((__, deleteEx) -> {
                            if (deleteEx != null)
                                lockFut.completeExceptionally(deleteEx);
                            else
                                lockFut.complete(null);
                        });
                    }

                    return null;
                });

        return lockFut;
    }


}
