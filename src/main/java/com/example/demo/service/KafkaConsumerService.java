package com.example.demo.service;

import com.example.demo.model.Customer;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Optional;

/**
 * Service responsible for consuming messages from Kafka topics.
 * Handles both Customer and Order messages with logging.
 */
@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
        topics = "orders",
        groupId = "order-group",
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(Order order) {
        log.info("Received order message from Kafka - Order ID: {}", order.getOrderId());
        log.debug("Order details - Customer: {}, Total Amount: {}, Status: {}", 
            Optional.ofNullable(order.getCustomer()).map(Customer::getName).orElse("N/A"),
            order.getTotalAmount(),
            order.getStatus()
        );
        
        // Log detailed information about items if they exist
        if (log.isDebugEnabled() && order.getItems() != null) {
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