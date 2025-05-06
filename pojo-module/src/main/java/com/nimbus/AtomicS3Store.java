package com.nimbus;

import com.nimbus.serializers.DataSerializer;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Enables convenient and safe storage of objects in S3. For example,
 * coordinating configuration updates from multiple servers to a shared data store.
 * Primary safety mechanism is tracking and validating an expected etag value,
 * which is required to update existing objects.
 * <br />
 * Allows for serialization to/from any class, which provides for easy
 * adaptation such as POJO storage.
 * @param <T> Class of object being stored
 */
public class AtomicS3Store<T> {

    private final S3AsyncClient client;
    private final DataSerializer<T> serializer;

    /**
     * Construct a store with the provided client and serializer.
     * @param s3AsyncClient A preconfigured {@link S3AsyncClient}
     * @param serializer {@link DataSerializer<T>} which handles translating the desired class
     *                                            to and from a byte array for storage
     */
    public AtomicS3Store(S3AsyncClient s3AsyncClient, DataSerializer<T> serializer) {
        if (s3AsyncClient == null)
            throw new NullPointerException("s3AsyncClient is null");

        this.client = s3AsyncClient;
        this.serializer = serializer;
    }

    /**
     * Check if an object exists, primarily used for retrieving initial etag stage
     * of an object
     * @param bucket Associated bucket
     * @param key Object key
     * @return A future which resolves to an {@link ObjectState<T>} May be used
     * fot future put calls which require the etag value for validation.
     * @implNote The data of the returned ObjectState will be null
     */
    public CompletableFuture<ObjectState<T>> exists(String bucket, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        return this.client.headObject(headObjectRequest).thenApply(res -> {
           return ObjectState.of(res.eTag(), bucket, key, null);
        });
    }

    /**
     * Retrieves an object from S3 and returns its state.
     *
     * @param bucket The S3 bucket name.
     * @param key    The S3 object key.
     * @return A future resolving to a {@link ObjectState<T>} with the current etag and data, or null
     * if no object was found
     * @implNote This only completes exceptionally on unexpected exceptions
     *
     * @throws S3Exception for other S3-related failures.
     */
    public CompletableFuture<ObjectState<T>> get(String bucket, String key) {
        if (bucket == null || key == null)
            throw new NullPointerException("bucket and key is null");

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        return client.getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ex.getCause();
                        if (cause instanceof NoSuchKeyException)
                            return null;

                        if (cause instanceof S3Exception s3e
                                && s3e.statusCode() == 404
                                && "NoSuchKey".equals(s3e.awsErrorDetails().errorCode())) {
                            return null;
                        }

                        // anything else—including NoSuchBucket—propagates
                        throw new CompletionException(cause);
                    }

                    T obj;
                    try {
                        obj = this.serializer.deserialize(res.asByteArray());
                    } catch (Throwable t) {
                        throw new IllegalStateException("Failed to deserialize object: " + t.getMessage(), t);
                    }

                    return ObjectState.of(
                            res.response().eTag(),
                            bucket,
                            key,
                            obj
                    );
                });
    }


    /**
     * Atomically updates an object in S3 using the stored ETag for conflict detection.
     * Will fail if the object already exists and the provided etag is null or does not match.
     *
     * @param state The object state, including previous ETag. A null etag value is acceptable
     *              during the initial put of a newly created object.
     * @return A future resolving to a new {@link ObjectState} with the updated ETag.
     *
     * @throws ConcurrentModificationException if the ETag no longer matches.
     * @throws CompletionException wrapping S3Exception for other errors.
     */
    public CompletableFuture<ObjectState<T>> put(ObjectState<T> state) {
        if (state == null)
            throw new NullPointerException("data is null");
        if (state.bucket() == null || state.key() == null)
            throw new NullPointerException("data bucket and key is null");

        PutObjectRequest.Builder putBuilder = PutObjectRequest.builder()
                .bucket(state.bucket())
                .key(state.key());

        if (state.etag() != null)
            putBuilder.ifMatch(state.etag());

        PutObjectRequest putObjectRequest = putBuilder.build();

        byte[] data;
        try {
            data = this.serializer.serialize(state.data());
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to serialize object: " + t.getMessage(), t);
        }

        return this.client.putObject(putObjectRequest, AsyncRequestBody.fromBytes(data))
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ex.getCause();
                        if (cause instanceof S3Exception s3e && s3e.statusCode() == 412)
                            throw new ConcurrentModificationException("Etag mismatch during put, data may have been modified", s3e);

                        throw new CompletionException(cause);
                    }

                    return ObjectState.of(
                            res.eTag(),
                            state.bucket(),
                            state.key(),
                            state.data()
                    );
                });
    }

    /**
     * Deletes an object from S3.
     *
     * @param state The object state, with optional etag for conditional delete.
     *              If etag is null, delete is unconditional.
     * @return A future that completes when the object is deleted.
     *
     * @throws ConcurrentModificationException if the etag no longer matches.
     * @throws CompletionException wrapping S3Exception for other errors.
     */
    public CompletableFuture<Void> delete(ObjectState<T> state) {
        if (state == null)
            throw new NullPointerException("state is null");
        if (state.bucket() == null || state.key() == null)
            throw new NullPointerException("state bucket and key is null");

        DeleteObjectRequest.Builder builder = DeleteObjectRequest.builder()
                .bucket(state.bucket())
                .key(state.key());

        if (state.etag() != null)
            builder.ifMatch(state.etag());

        DeleteObjectRequest deleteRequest = builder.build();

        return client.deleteObject(deleteRequest)
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ex.getCause();
                        if (cause instanceof S3Exception s3e && s3e.statusCode() == 412)
                            throw new ConcurrentModificationException("Etag mismatch during delete, object may have been modified", s3e);

                        throw new CompletionException(cause);
                    }

                    return null;
                });
    }

}
