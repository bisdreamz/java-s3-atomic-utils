package com.nimbus.serializers;

public class ByteArraySerializer implements DataSerializer<byte[]> {

    @Override
    public byte[] serialize(byte[] obj) {
        return obj;
    }

    @Override
    public byte[] deserialize(byte[] bytes) {
        return bytes;
    }

}
