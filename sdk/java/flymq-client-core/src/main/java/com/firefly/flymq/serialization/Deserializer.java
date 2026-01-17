package com.firefly.flymq.serialization;

public interface Deserializer<T> {
    T deserialize(String topic, byte[] data);
    
    default void close() {}
}
