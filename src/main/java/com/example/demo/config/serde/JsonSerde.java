package com.example.demo.config.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {
    private final Class<T> type;
    private final ObjectMapper mapper;

    public JsonSerde(Class<T> type) {
        this.type = type;
        this.mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return data != null ? mapper.writeValueAsBytes(data) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error serializing value", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            try {
                return data != null ? mapper.readValue(data, type) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing value", e);
            }
        };
    }
} 