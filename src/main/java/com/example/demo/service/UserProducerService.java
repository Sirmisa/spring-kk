package com.example.demo.service;

import com.example.demo.model.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserProducerService {

    private final KafkaTemplate<String, User> kafkaTemplate;
    private static final String TOPIC = "users";

    public void sendUser(User user) {
        log.info("Sending user message to Kafka: {}", user);
        CompletableFuture<SendResult<String, User>> future = kafkaTemplate.send(TOPIC, String.valueOf(user.getId()), user);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                log.error("Unable to send message due to : " + ex.getMessage());
            }
        });
    }
} 