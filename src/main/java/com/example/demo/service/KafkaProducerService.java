package com.example.demo.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for sending messages to Kafka topics.
 * Supports sending any type of message using generic JSON serialization.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Sends a message to a specified Kafka topic.
     * @param topic The topic to send the message to
     * @param key The message key
     * @param message The message payload
     * @param <T> The type of the message payload
     */
    public <T> void sendMessage(String topic, String key, T message) {
        log.info("Sending message to topic: {}, key: {}", topic, key);
        log.debug("Message payload: {}", message);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, message);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully sent message to topic: {}, partition: {}, offset: {}",
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message to topic: {}", topic, ex);
            }
        });
    }
} 