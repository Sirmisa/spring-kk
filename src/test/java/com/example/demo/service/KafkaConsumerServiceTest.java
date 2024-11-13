package com.example.demo.service;

import com.example.demo.config.KafkaTestConfig;
import com.example.demo.model.User;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import com.example.demo.model.Address;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=test-group",
    "spring.main.allow-bean-definition-overriding=true"
})
@EmbeddedKafka(partitions = 1, 
               brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
               topics = {"users", "orders"})
@Import(KafkaTestConfig.class)
@DirtiesContext
@TestPropertySource(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.main.allow-bean-definition-overriding=true"
})
class KafkaConsumerServiceTest {

    @Autowired
    private KafkaProducerService producerService;

    @SpyBean
    private KafkaConsumerService consumerService;

    private User testUser;
    private Order testOrder;

    @BeforeEach
    void setup() {
        // Setup test user
        testUser = new User(1L, "Test User", "test@example.com");

        // Setup test order
        testOrder = new Order();
        testOrder.setOrderId("TEST-001");
        testOrder.setOrderDate(LocalDateTime.now());
        testOrder.setCustomer(testUser);
        
        Address address = new Address();
        address.setStreet("123 Test St");
        address.setCity("Test City");
        address.setState("TS");
        address.setCountry("Test Country");
        address.setZipCode("12345");
        
        testOrder.setShippingAddress(address);
        testOrder.setBillingAddress(address);
        
        OrderItem item = new OrderItem();
        item.setProductId(1L);
        item.setProductName("Test Product");
        item.setQuantity(2);
        item.setUnitPrice(new BigDecimal("49.99"));
        item.setTotalPrice(new BigDecimal("99.98"));

        testOrder.setItems(Arrays.asList(item));
        testOrder.setTotalAmount(new BigDecimal("99.98"));
        testOrder.setStatus("PENDING");
    }

    @Test
    void testConsumeUser() {
        producerService.sendMessage("users", testUser.getId().toString(), testUser);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
            });
    }

    @Test
    void testConsumeOrder() {
        producerService.sendMessage("orders", testOrder.getOrderId(), testOrder);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testConsumeMixedMessages() {
        producerService.sendMessage("users", testUser.getId().toString(), testUser);
        producerService.sendMessage("orders", testOrder.getOrderId(), testOrder);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testConsumeMultipleMessages() {
        User secondUser = new User(2L, "Second User", "second@example.com");
        Order secondOrder = new Order();
        secondOrder.setOrderId("TEST-002");
        secondOrder.setCustomer(secondUser);
        secondOrder.setStatus("PENDING");

        producerService.sendMessage("users", testUser.getId().toString(), testUser);
        producerService.sendMessage("users", secondUser.getId().toString(), secondUser);
        producerService.sendMessage("orders", testOrder.getOrderId(), testOrder);
        producerService.sendMessage("orders", secondOrder.getOrderId(), secondOrder);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeUser(any(User.class));
                verify(consumerService, times(2)).consumeOrder(any(Order.class));
            });
    }
} 