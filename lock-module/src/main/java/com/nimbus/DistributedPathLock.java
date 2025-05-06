package com.nimbus;

import com.nimbus.exceptions.LocalLockExistsException;
import com.nimbus.exceptions.RemoteLockExistsException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A convenience wrapper around {@link DistributedLock} for lock(s) which share
 * a bucket directory. E.g. /globalLocks may have /globalLocks/1.lock, /globalLocks/2.lock
 * and so forth. Prevents requiring to pass the bucket during every lock call.
 */
public class DistributedPathLock {

    private final DistributedLock distributedLock;
    private final String bucket;

    /**
     * Construct a path lock with the fixed lock path
     * @param distributedLock The backing {@link DistributedLock} instance
     * @param bucket The fixed path (bucket) associated with the distributed path lock instance
     */
    private DistributedPathLock(DistributedLock distributedLock, String bucket) {
        if (distributedLock == null)
            throw new NullPointerException("DistributedLock is null");
        if (bucket == null || bucket.isEmpty())
            throw new NullPointerException("Bucket is null or empty");

        this.distributedLock = distributedLock;
        this.bucket = bucket;
    }

    public static DistributedPathLock of(DistributedLock distributedLock, String bucket) {
        return new DistributedPathLock(distributedLock, bucket);
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
     * @param lockkey          the name (as s3 key) for this lock under the associated bucket
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
            String lockkey,
            Duration expiryInterval,
            Duration maxLockDuration,
            Supplier<CompletableFuture<Void>> lockBody
    ) {
        return this.distributedLock.lock(new LockEntry(this.bucket, lockkey),
                expiryInterval,
                maxLockDuration,
                lockBody);
    }

    /**
     * @return The associated bucket underneath which all locks will be placed
     */
    public String getBucket() {
        return this.bucket;
    }

}
