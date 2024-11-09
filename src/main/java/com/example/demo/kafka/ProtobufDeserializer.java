package com.example.demo.kafka;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtobufDeserializer<T extends Message> implements Deserializer<T> {
    private final Parser<T> parser;

    public ProtobufDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return parser.parseFrom(data);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Protobuf message", e);
        }
    }
} 