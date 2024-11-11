package com.example.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private String orderId;
    private LocalDateTime orderDate;
    private Customer customer;
    private Address shippingAddress;
    private Address billingAddress;
    private List<OrderItem> items;
    private double totalAmount;
    private String status;
} 