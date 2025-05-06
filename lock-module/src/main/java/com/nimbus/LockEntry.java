package com.nimbus;

public record LockEntry(String bucket, String name) {

    public static final String SUFFIX = ".lock";

    public static LockEntry of(String bucket, String name) {
        return new LockEntry(bucket, name);
    }

    public String fileName() {
        return this.name.endsWith(".lock") ? this.name : (this.name + SUFFIX);
    }

    public String absUri() {
        return this.bucket
                + (this.bucket.endsWith("/") ? "" : "/")
                + this.fileName();
    }

}
