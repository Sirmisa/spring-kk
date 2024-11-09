package com.example.demo.controller;

import com.example.demo.model.User;
import com.example.demo.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for handling user-related requests.
 * Provides endpoints for sending user messages to Kafka.
 */
@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final KafkaProducerService producerService;
    private static final Logger log = LoggerFactory.getLogger(UserController.class);

    /**
     * Endpoint for sending a user message to Kafka.
     * @param user The user object to be sent
     */
    @PostMapping
    public void sendUser(@RequestBody User user) {
        log.info("Received request to send user message");
        log.debug("User details: {}", user);
        producerService.sendMessage("users", String.valueOf(user.getId()), user);
    }
} 