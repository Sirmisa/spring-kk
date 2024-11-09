package com.example.demo.service;

import com.example.demo.model.User;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Service responsible for consuming messages from Kafka topics.
 * Handles both User and Order messages with logging.
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

    @KafkaListener(
        topics = "orders",
        groupId = "order-group",
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(Order order) {
        log.info("Received order message from Kafka - Order ID: {}", order.getOrderId());
        log.debug("Order details - Customer: {}, Total Amount: {}, Status: {}", 
            order.getCustomer().getName(),
            order.getTotalAmount(),
            order.getStatus()
        );
        
        // Log detailed information about items
        if (log.isDebugEnabled()) {
            order.getItems().forEach(item -> 
                log.debug("Order item - Product: {}, Quantity: {}, Total: {}", 
                    item.getProductName(),
                    item.getQuantity(),
                    item.getTotalPrice()
                )
            );
        }
    }
} 