package com.firefly.flymq.serialization;

public interface Serializer<T> {
    byte[] serialize(String topic, T data);
    
    default void close() {}
}
