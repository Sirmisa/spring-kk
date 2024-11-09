package com.example.demo.controller;

import com.example.demo.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST Controller for handling User-related HTTP requests
 * Provides endpoints for creating and managing users
 */
@RestController
@RequestMapping("/api")
public class UserController {
    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    /**
     * Creates a new user and publishes it to Kafka
     * @param user The user object from the request body
     * @return The created user object
     */
    @PostMapping("/users")
    public ResponseEntity<Void> createUser(@RequestBody User user) {
        logger.info("Received request to create user: {}", user);
        
        // Publish the user to Kafka topic
        String key = String.valueOf(user.getId());
        logger.debug("Publishing user to Kafka topic 'users-input' with key: {}", key);
        
        kafkaTemplate.send("users-input", key, user)
            .thenAccept(result -> logger.info("Successfully published user to Kafka: {}", user))
            .exceptionally(ex -> {
                logger.error("Failed to publish user to Kafka: {}", ex.getMessage());
                return null;
            });
        
        return ResponseEntity.accepted().build();
    }
} 