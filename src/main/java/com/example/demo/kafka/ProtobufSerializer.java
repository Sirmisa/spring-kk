package com.example.demo.kafka;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class ProtobufSerializer<T extends Message> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            log.debug("Null received at serializing");
            return null;
        }
        try {
            log.debug("Serializing message for topic {}", topic);
            byte[] result = data.toByteArray();
            log.trace("Successfully serialized message: {}", data);
            return result;
        } catch (Exception e) {
            log.error("Error when serializing Protobuf message to byte[]", e);
            throw new SerializationException("Error serializing Protobuf message", e);
        }
    }
} 