package com.example.demo.service;

import com.example.demo.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UserConsumerService {

    @KafkaListener(topics = "users", groupId = "user-group")
    public void consume(@Payload User user) {
        log.info("------------------------");
        log.info("Received user message in consumer");
        log.info("User ID: {}", user.getId());
        log.info("User Name: {}", user.getName());
        log.info("User Email: {}", user.getEmail());
        log.info("------------------------");
    }
} 