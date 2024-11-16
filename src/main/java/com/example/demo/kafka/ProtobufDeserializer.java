package com.example.demo.kafka;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class ProtobufDeserializer<T extends Message> implements Deserializer<T> {
    private final Parser<T> parser;

    public ProtobufDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            log.debug("Null received at deserializing");
            return null;
        }
        try {
            log.debug("Deserializing message from topic {}", topic);
            T result = parser.parseFrom(data);
            log.trace("Successfully deserialized message: {}", result);
            return result;
        } catch (Exception e) {
            log.error("Error when deserializing byte[] to Protobuf message", e);
            throw new SerializationException("Error deserializing Protobuf message", e);
        }
    }
} 