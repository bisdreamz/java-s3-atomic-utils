package com.nimbus;

public record ObjectState<T>(String etag, String bucket, String key, T data) {

    public static <T> ObjectState<T> of(String etag, String bucket, String key, T data) {
        return new ObjectState<>(etag, bucket, key, data);
    }

}
