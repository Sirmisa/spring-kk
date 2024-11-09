package com.example.demo.controller;

import com.example.demo.model.Order;
import com.example.demo.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for handling order-related requests.
 * Provides endpoints for sending order messages to Kafka.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final KafkaProducerService producerService;

    /**
     * Endpoint for sending an order message to Kafka.
     * @param order The order object to be sent
     */
    @PostMapping
    public void sendOrder(@RequestBody Order order) {
        log.info("Received request to send order message - Order ID: {}", order.getOrderId());
        log.debug("Order details: {}", order);
        producerService.sendMessage("orders", order.getOrderId(), order);
    }
} 