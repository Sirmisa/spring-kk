package com.example.demo.config;

import com.example.demo.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Serde implementation for Order class
 * Handles serialization and deserialization of Order objects for Kafka Streams
 */
public class OrderSerde implements Serde<Order> {
    private static final Logger logger = LoggerFactory.getLogger(OrderSerde.class);
    private final ObjectMapper objectMapper;

    public OrderSerde() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule()); // For LocalDateTime serialization
    }

    @Override
    public Serializer<Order> serializer() {
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Order order) {
                try {
                    logger.debug("Serializing order for topic {}: {}", topic, order);
                    byte[] data = objectMapper.writeValueAsBytes(order);
                    logger.debug("Serialized order to {} bytes", data.length);
                    return data;
                } catch (Exception e) {
                    logger.error("Error serializing order: {}", order, e);
                    throw new RuntimeException("Error serializing Order", e);
                }
            }
        };
    }

    @Override
    public Deserializer<Order> deserializer() {
        return new Deserializer<>() {
            @Override
            public Order deserialize(String topic, byte[] data) {
                try {
                    logger.debug("Deserializing order data from topic {}, data size: {} bytes", 
                        topic, data.length);
                    Order order = objectMapper.readValue(data, Order.class);
                    logger.debug("Successfully deserialized order: {}", order);
                    return order;
                } catch (Exception e) {
                    logger.error("Error deserializing order data from topic {}", topic, e);
                    throw new RuntimeException("Error deserializing Order", e);
                }
            }
        };
    }
} 