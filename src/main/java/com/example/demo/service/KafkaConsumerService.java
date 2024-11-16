package com.example.demo.service;

import com.example.demo.proto.OrderProto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
        topics = "orders",
        groupId = "order-group",
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(OrderProto order) {
        log.info("Received order message from Kafka - Order ID: {}", order.getOrderId());
        log.debug("Order details - Customer: {}, Total Amount: {}, Status: {}", 
            order.hasCustomer() ? order.getCustomer().getName() : "N/A",
            order.getTotalAmount(),
            order.getStatus()
        );
        
        if (order.getItemsCount() > 0) {
            order.getItemsList().forEach(item -> 
                log.debug("Order item - Product: {}, Quantity: {}, Total: {}", 
                    item.getProductName(),
                    item.getQuantity(),
                    item.getTotalPrice()
                )
            );
        }
    }
} 