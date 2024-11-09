package com.example.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private String orderId;
    private LocalDateTime orderDate;
    private User customer;
    private Address shippingAddress;
    private Address billingAddress;
    private List<OrderItem> items;
    private BigDecimal totalAmount;
    private String status;
} 