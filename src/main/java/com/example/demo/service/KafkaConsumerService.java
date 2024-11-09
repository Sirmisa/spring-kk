package com.example.demo.service;

import com.example.demo.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Service responsible for consuming messages from Kafka topics.
 * Currently handles User messages with logging.
 */
@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
        topics = "users",
        groupId = "user-group",
        containerFactory = "userKafkaListenerContainerFactory"
    )
    public void consumeUser(User user) {
        log.info("Received user message from Kafka");
        log.debug("User details - ID: {}, Name: {}, Email: {}", 
            user.getId(), 
            user.getName(), 
            user.getEmail()
        );
    }
} 