package com.example.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderItem {
    private Long productId;
    private String productName;
    private int quantity;
    private double unitPrice;
    private double totalPrice;
} 