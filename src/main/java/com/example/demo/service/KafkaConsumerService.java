package com.example.demo.service;

import com.example.demo.model.Customer;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.stream.Collectors;

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
        
        // Basic order details
        log.debug("Order details - ID: {}, Date: {}, Status: {}, Total Amount: {}", 
            order.getOrderId(), 
            order.getOrderDate(), 
            order.getStatus(), 
            order.getTotalAmount()
        );

        // Customer details
        Optional.ofNullable(order.getCustomer())
            .ifPresent(customer -> log.debug("Customer details - ID: {}, Name: {}, Email: {}", 
                customer.getId(), 
                customer.getName(), 
                customer.getEmail()
            ));

        // Shipping address
        Optional.ofNullable(order.getShippingAddress())
            .ifPresent(address -> log.debug("Shipping address - Street: {}, City: {}, State: {}, Zip: {}, Country: {}", 
                address.getStreet(), 
                address.getCity(), 
                address.getState(), 
                address.getZipCode(), 
                address.getCountry()
            ));

        // Billing address
        Optional.ofNullable(order.getBillingAddress())
            .ifPresent(address -> log.debug("Billing address - Street: {}, City: {}, State: {}, Zip: {}, Country: {}", 
                address.getStreet(), 
                address.getCity(), 
                address.getState(), 
                address.getZipCode(), 
                address.getCountry()
            ));

        // Order items
        Optional.ofNullable(order.getItems())
            .ifPresent(items -> items.forEach(item -> 
                log.debug("Order item - Product: {} (ID: {}), Quantity: {}, Unit Price: {}, Total: {}", 
                    item.getProductName(),
                    item.getProductId(),
                    item.getQuantity(),
                    item.getUnitPrice(),
                    item.getTotalPrice()
                )
            ));
    }
} 