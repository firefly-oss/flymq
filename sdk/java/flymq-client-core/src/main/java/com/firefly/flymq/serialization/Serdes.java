package com.firefly.flymq.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Serdes {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Serializer<String> StringSerializer() {
        return (topic, data) -> data == null ? null : data.getBytes(StandardCharsets.UTF_8);
    }

    public static Deserializer<String> StringDeserializer() {
        return (topic, data) -> data == null ? null : new String(data, StandardCharsets.UTF_8);
    }

    public static Serializer<byte[]> BytesSerializer() {
        return (topic, data) -> data;
    }

    public static Deserializer<byte[]> BytesDeserializer() {
        return (topic, data) -> data;
    }

    public static <T> Serializer<T> JsonSerializer(Class<T> clazz) {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error serializing JSON", e);
            }
        };
    }

    public static <T> Deserializer<T> JsonDeserializer(Class<T> clazz) {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, clazz);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing JSON", e);
            }
        };
    }
}
