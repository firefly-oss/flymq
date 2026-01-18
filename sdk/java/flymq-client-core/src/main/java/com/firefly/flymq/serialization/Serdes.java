package com.firefly.flymq.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Serdes {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, Serializer<?>> SERIALIZERS = new ConcurrentHashMap<>();
    private static final Map<String, Deserializer<?>> DESERIALIZERS = new ConcurrentHashMap<>();

    static {
        register("string", StringSerializer(), StringDeserializer());
        register("binary", BytesSerializer(), BytesDeserializer());
        register("json", JsonSerializer(Object.class), JsonDeserializer(Object.class));
        register("avro", AvroSerializer(), AvroDeserializer());
        register("protobuf", ProtoSerializer(), ProtoDeserializer());
    }

    public static void register(String name, Serializer<?> serializer, Deserializer<?> deserializer) {
        SERIALIZERS.put(name, serializer);
        DESERIALIZERS.put(name, deserializer);
    }

    public static Serializer<?> getSerializer(String name) {
        return SERIALIZERS.get(name);
    }

    public static Deserializer<?> getDeserializer(String name) {
        return DESERIALIZERS.get(name);
    }

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

    public static Serializer<Object> AvroSerializer(String schemaStr) {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaStr);
        return (topic, data) -> {
            if (data == null) return null;
            try {
                org.apache.avro.io.DatumWriter<Object> writer = new org.apache.avro.generic.GenericDatumWriter<>(schema);
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                org.apache.avro.io.Encoder encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(out, null);
                writer.write(data, encoder);
                encoder.flush();
                return out.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Error serializing Avro", e);
            }
        };
    }

    public static Deserializer<Object> AvroDeserializer(String schemaStr) {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaStr);
        return (topic, data) -> {
            if (data == null) return null;
            try {
                org.apache.avro.io.DatumReader<Object> reader = new org.apache.avro.generic.GenericDatumReader<>(schema);
                org.apache.avro.io.Decoder decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(data, null);
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing Avro", e);
            }
        };
    }

    public static <T extends com.google.protobuf.Message> Serializer<T> ProtoSerializer() {
        return (topic, data) -> data == null ? null : data.toByteArray();
    }

    public static <T extends com.google.protobuf.Message> Deserializer<T> ProtoDeserializer(T defaultInstance) {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return (T) defaultInstance.newBuilderForType().mergeFrom(data).build();
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new RuntimeException("Error deserializing Protobuf", e);
            }
        };
    }

    // Default registry-compatible versions (stubs that throw if no schema is provided)
    public static Serializer<Object> AvroSerializer() {
        return (topic, data) -> { throw new RuntimeException("Avro serialization requires a schema. Use AvroSerializer(schemaStr)"); };
    }

    public static Deserializer<Object> AvroDeserializer() {
        return (topic, data) -> { throw new RuntimeException("Avro deserialization requires a schema. Use AvroDeserializer(schemaStr)"); };
    }

    public static Serializer<Object> ProtoSerializer() {
        return (topic, data) -> {
            if (data instanceof com.google.protobuf.Message) {
                return ((com.google.protobuf.Message) data).toByteArray();
            }
            throw new RuntimeException("Protobuf serialization requires a com.google.protobuf.Message");
        };
    }

    public static Deserializer<Object> ProtoDeserializer() {
        return (topic, data) -> { throw new RuntimeException("Protobuf deserialization requires a specific message type. Use ProtoDeserializer(defaultInstance)"); };
    }
}
