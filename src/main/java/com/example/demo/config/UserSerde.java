package com.example.demo.config;

import com.example.demo.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Custom Serde implementation for User class
 * Handles serialization and deserialization of User objects for Kafka Streams
 */
public class UserSerde implements Serde<User> {
    private static final Logger logger = LoggerFactory.getLogger(UserSerde.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<User> serializer() {
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, User user) {
                try {
                    logger.debug("Serializing user for topic {}: {}", topic, user);
                    byte[] data = objectMapper.writeValueAsBytes(user);
                    logger.debug("Serialized user to {} bytes", data.length);
                    return data;
                } catch (Exception e) {
                    logger.error("Error serializing user: {}", user, e);
                    throw new RuntimeException("Error serializing User", e);
                }
            }
        };
    }

    @Override
    public Deserializer<User> deserializer() {
        return new Deserializer<>() {
            @Override
            public User deserialize(String topic, byte[] data) {
                try {
                    logger.debug("Deserializing user data from topic {}, data size: {} bytes", 
                        topic, data.length);
                    User user = objectMapper.readValue(data, User.class);
                    logger.debug("Successfully deserialized user: {}", user);
                    return user;
                } catch (Exception e) {
                    logger.error("Error deserializing user data from topic {}", topic, e);
                    throw new RuntimeException("Error deserializing User", e);
                }
            }
        };
    }
} 