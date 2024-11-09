package com.example.demo.controller;

import com.example.demo.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for handling Order-related HTTP requests
 */
@RestController
@RequestMapping("/api")
public class OrderController {
    private static final Logger logger = LoggerFactory.getLogger(OrderController.class);

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @PostMapping("/orders")
    public ResponseEntity<Void> createOrder(@RequestBody Order order) {
        logger.info("Received request to create order: {}", order);
        
        String key = order.getOrderId();
        logger.debug("Publishing order to Kafka topic 'orders-input' with key: {}", key);
        
        kafkaTemplate.send("orders-input", key, order)
            .thenAccept(result -> logger.info("Successfully published order to Kafka: {}", order))
            .exceptionally(ex -> {
                logger.error("Failed to publish order to Kafka: {}", ex.getMessage());
                return null;
            });
        
        return ResponseEntity.accepted().build();
    }
} 