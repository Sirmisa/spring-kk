package com.example.demo.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic JSON deserializer that can convert JSON to any specified Java type.
 * Uses Jackson ObjectMapper for JSON conversion.
 * Supports Java 8 date/time types through JavaTimeModule.
 */
@Slf4j
public class GenericJsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper;
    private final Class<T> targetType;

    public GenericJsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        log.debug("Initializing deserializer for type: {} with JavaTimeModule", targetType.getSimpleName());
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            log.debug("Null received at deserializing");
            return null;
        }
        try {
            log.debug("Deserializing message from topic {} to type {}", topic, targetType.getSimpleName());
            T result = objectMapper.readValue(data, targetType);
            log.trace("Successfully deserialized message: {}", result);
            return result;
        } catch (Exception e) {
            log.error("Error when deserializing byte[] to {}", targetType.getSimpleName(), e);
            throw new SerializationException("Error deserializing JSON message", e);
        }
    }
} 