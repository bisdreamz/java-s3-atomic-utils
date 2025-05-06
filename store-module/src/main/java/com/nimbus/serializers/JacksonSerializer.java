package com.nimbus.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Provides a to & from pojo serializer for the object store
 */
public class JacksonSerializer<T> implements DataSerializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> clazz;

    /**
     * Construct a serializer using a mapper with the provided settings
     * @param objectMapper {@link ObjectMapper} from Jackson
     */
    public JacksonSerializer(Class<T> clazz, ObjectMapper objectMapper) {
        this.clazz = clazz;

        this.objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
    }

    /**
     * Construct a serializer with the default Jackson settings
     */
    public JacksonSerializer(Class<T> clazz) {
        this(clazz, null);
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        return this.objectMapper.writeValueAsBytes(obj);
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        return this.objectMapper.readValue(bytes, this.clazz);
    }

}
