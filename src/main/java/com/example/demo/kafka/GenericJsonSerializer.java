package com.example.demo.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic JSON serializer that can handle any Java object type.
 * Uses Jackson ObjectMapper for JSON conversion.
 * Supports Java 8 date/time types through JavaTimeModule.
 */
@Slf4j
public class GenericJsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper;

    public GenericJsonSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        log.debug("Initialized GenericJsonSerializer with JavaTimeModule");
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            log.debug("Null received at serializing");
            return null;
        }
        try {
            log.debug("Serializing {} to topic {}", data.getClass().getSimpleName(), topic);
            byte[] retVal = objectMapper.writeValueAsBytes(data);
            log.trace("Serialized payload size: {} bytes", retVal.length);
            return retVal;
        } catch (Exception e) {
            log.error("Error when serializing {} to byte[]", data.getClass().getSimpleName(), e);
            throw new SerializationException("Error serializing JSON message", e);
        }
    }
} 