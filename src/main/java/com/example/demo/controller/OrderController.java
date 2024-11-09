package com.example.demo.controller;

import com.example.demo.proto.Order;
import com.example.demo.proto.Customer;
import com.example.demo.proto.Address;
import com.example.demo.proto.OrderItem;
import com.example.demo.proto.OrderStatus;
import com.example.demo.service.OrderKafkaService;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderKafkaService orderKafkaService;

    @PostMapping
    public void sendOrder(@RequestBody OrderDto orderDto) {
        Order order = convertToProtoOrder(orderDto);
        orderKafkaService.sendOrder(order);
    }

    private Order convertToProtoOrder(OrderDto orderDto) {
        return Order.newBuilder()
            .setOrderId(orderDto.orderId())
            .setOrderDate(convertToTimestamp(orderDto.orderDate()))
            .setCustomer(Customer.newBuilder()
                .setId(orderDto.customer().id())
                .setName(orderDto.customer().name())
                .setEmail(orderDto.customer().email())
                .build())
            .setShippingAddress(convertToAddress(orderDto.shippingAddress()))
            .setBillingAddress(convertToAddress(orderDto.billingAddress()))
            .addAllItems(orderDto.items().stream()
                .map(this::convertToOrderItem)
                .toList())
            .setTotalAmount(orderDto.totalAmount())
            .setStatus(OrderStatus.valueOf("ORDER_STATUS_" + orderDto.status()))
            .build();
    }

    private Timestamp convertToTimestamp(String dateStr) {
        LocalDateTime dateTime = LocalDateTime.parse(dateStr);
        Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }

    private Address convertToAddress(AddressDto addressDto) {
        return Address.newBuilder()
            .setStreet(addressDto.street())
            .setCity(addressDto.city())
            .setState(addressDto.state())
            .setCountry(addressDto.country())
            .setZipCode(addressDto.zipCode())
            .build();
    }

    private OrderItem convertToOrderItem(OrderItemDto itemDto) {
        return OrderItem.newBuilder()
            .setProductId(itemDto.productId())
            .setProductName(itemDto.productName())
            .setQuantity(itemDto.quantity())
            .setUnitPrice(itemDto.unitPrice())
            .setTotalPrice(itemDto.totalPrice())
            .build();
    }
}

record OrderDto(
    String orderId,
    String orderDate,
    CustomerDto customer,
    AddressDto shippingAddress,
    AddressDto billingAddress,
    java.util.List<OrderItemDto> items,
    double totalAmount,
    String status
) {}

record CustomerDto(int id, String name, String email) {}

record AddressDto(
    String street,
    String city,
    String state,
    String country,
    String zipCode
) {}

record OrderItemDto(
    int productId,
    String productName,
    int quantity,
    double unitPrice,
    double totalPrice
) {} 