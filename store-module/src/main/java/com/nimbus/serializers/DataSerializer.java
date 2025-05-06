package com.nimbus.serializers;

import java.io.IOException;

public interface DataSerializer<T> {

    public byte[] serialize(T obj) throws IOException;

    public T deserialize(byte[] bytes) throws IOException;

}
