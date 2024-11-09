package com.example.demo.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic JSON serializer that can handle any Java object type.
 * Uses Jackson ObjectMapper for JSON conversion.
 */
@Slf4j
public class GenericJsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

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