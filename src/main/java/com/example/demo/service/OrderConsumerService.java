package com.example.demo.service;

import com.example.demo.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderConsumerService {

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(@Payload Order order) {
        log.info("------------------------");
        log.info("Received order in consumer - OrderId: {}", order.getOrderId());
        log.info("Order Details:");
        log.info("Date: {}", order.getOrderDate());
        log.info("Customer: {} ({})", order.getCustomer().getName(), order.getCustomer().getEmail());
        log.info("Total Amount: ${}", order.getTotalAmount());
        log.info("Status: {}", order.getStatus());
        log.info("Number of items: {}", order.getItems().size());
        log.info("------------------------");
    }
} 