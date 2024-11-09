package com.example.demo.service;

import com.example.demo.proto.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserKafkaService {

    private final KafkaTemplate<String, User> kafkaTemplate;
    private static final String TOPIC = "users";

    public void sendUser(User user) {
        kafkaTemplate.send(TOPIC, String.valueOf(user.getId()), user);
        log.debug("Sent user to Kafka: {}", user);
    }

    @KafkaListener(topics = TOPIC, groupId = "user-group")
    public void receiveUser(User user) {
        log.debug("Received user from Kafka: {}", user);
        // Process the user message as needed
    }
} 