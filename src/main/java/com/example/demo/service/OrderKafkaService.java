package com.example.demo.service;

import com.example.demo.proto.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderKafkaService {

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private static final String TOPIC = "orders";

    public void sendOrder(Order order) {
        kafkaTemplate.send(TOPIC, order.getOrderId(), order);
        log.debug("Sent order to Kafka: {}", order);
    }

    @KafkaListener(
        topics = TOPIC, 
        groupId = "order-group",
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void receiveOrder(Order order) {
        log.debug("Received order from Kafka: {}", order);
        // Process the order message as needed
    }
} 