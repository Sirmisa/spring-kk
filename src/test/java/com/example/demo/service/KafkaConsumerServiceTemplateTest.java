package com.example.demo.service;

import com.example.demo.config.KafkaConfig;
import com.example.demo.config.SerdesConfig;
import com.example.demo.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=test-group"
})
@EmbeddedKafka(partitions = 1,
    brokerProperties = {"listeners=PLAINTEXT://localhost:9000"},
    topics = {"orders"})
@Import({KafkaConfig.class, SerdesConfig.class})
@DirtiesContext
class KafkaConsumerServiceTemplateTest {

    @Autowired
    private KafkaTemplate<String, Order> orderKafkaTemplate;

    @SpyBean
    private KafkaConsumerService consumerService;

    private Order testOrder;

    @BeforeEach
    void setup() {
        // Create test customer
        Customer testCustomer = new Customer(1L, "Test Customer", "test@example.com");

        // Create test addresses
        Address shippingAddress = new Address(
            "123 Shipping St",
            "Ship City",
            "Ship State",
            "12345",
            "Ship Country"
        );

        Address billingAddress = new Address(
            "456 Billing St",
            "Bill City",
            "Bill State",
            "67890",
            "Bill Country"
        );

        // Create test order items
        List<OrderItem> items = Arrays.asList(
            new OrderItem(1L, "Product 1", 2, BigDecimal.valueOf(29.99), BigDecimal.valueOf(59.98)),
            new OrderItem(2L, "Product 2", 1, BigDecimal.valueOf(49.99), BigDecimal.valueOf(49.99))
        );

        // Create test order
        testOrder = new Order();
        testOrder.setOrderId("TEST-001");
        testOrder.setOrderDate(LocalDateTime.now());
        testOrder.setCustomer(testCustomer);
        testOrder.setShippingAddress(shippingAddress);
        testOrder.setBillingAddress(billingAddress);
        testOrder.setItems(items);
        testOrder.setTotalAmount(BigDecimal.valueOf(109.97));
        testOrder.setStatus("PENDING");
    }

    @Test
    void shouldConsumeOrderMessage() {
        // When
        orderKafkaTemplate.send("orders", testOrder.getOrderId(), testOrder);

        // Then
        verify(consumerService, timeout(1000))
            .consumeOrder(argThat(order -> 
                order.getOrderId().equals(testOrder.getOrderId()) &&
                order.getStatus().equals(testOrder.getStatus()) &&
                order.getTotalAmount().compareTo(testOrder.getTotalAmount()) == 0 &&
                order.getCustomer().getName().equals(testOrder.getCustomer().getName()) &&
                order.getItems().size() == testOrder.getItems().size() &&
                order.getShippingAddress().getStreet().equals(testOrder.getShippingAddress().getStreet()) &&
                order.getBillingAddress().getStreet().equals(testOrder.getBillingAddress().getStreet())
            ));
    }
} 