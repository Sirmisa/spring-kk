package com.example.demo.service;

import com.example.demo.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProducerService {

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private static final String TOPIC = "orders";

    /**
     * Sends an order to Kafka topic
     * @param order The order to be sent
     */
    public void sendOrder(Order order) {
        log.info("Attempting to send order to Kafka - OrderId: {}", order.getOrderId());
        log.debug("Full order details: {}", order);

        CompletableFuture<SendResult<String, Order>> future = 
            kafkaTemplate.send(TOPIC, order.getOrderId(), order);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully sent order to Kafka - OrderId: {}, Offset: {}", 
                    order.getOrderId(), 
                    result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order to Kafka - OrderId: {}, Error: {}", 
                    order.getOrderId(), 
                    ex.getMessage(), 
                    ex);
            }
        });
    }
} 