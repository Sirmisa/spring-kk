package com.example.demo.controller;

import com.example.demo.model.Order;
import com.example.demo.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderProducerService producerService;

    /**
     * Endpoint to create a new order
     * @param order The order details from the request body
     * @return ResponseEntity with appropriate status
     */
    @PostMapping
    public ResponseEntity<Void> createOrder(@RequestBody Order order) {
        log.info("Received request to create order - OrderId: {}", order.getOrderId());
        producerService.sendOrder(order);
        return ResponseEntity.accepted().build();
    }
} 