package com.example.demo.controller;

import com.example.demo.config.KafkaTestConfig;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import com.example.demo.model.User;
import com.example.demo.model.Address;
import com.example.demo.service.KafkaConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=test-group",
    "spring.main.allow-bean-definition-overriding=true"
})
@EmbeddedKafka(partitions = 1, 
               brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
               topics = {"orders"})
@Import(KafkaTestConfig.class)
@DirtiesContext
@TestPropertySource(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.main.allow-bean-definition-overriding=true"
})
class OrderControllerTest {

    @Autowired
    private OrderController orderController;

    @SpyBean
    private KafkaConsumerService consumerService;

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMvc;
    private Order testOrder;
    private User testUser;

    @BeforeEach
    void setup() {
        mockMvc = MockMvcBuilders.standaloneSetup(orderController).build();
        
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
    void testSendOrder() throws Exception {
        mockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testOrder)))
                .andExpect(status().isOk());

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testSendMultipleOrders() throws Exception {
        Order secondOrder = new Order();
        secondOrder.setOrderId("TEST-002");
        secondOrder.setOrderDate(LocalDateTime.now());
        secondOrder.setCustomer(testUser);
        secondOrder.setStatus("PENDING");
        secondOrder.setTotalAmount(new BigDecimal("50.00"));
        
        OrderItem item = new OrderItem();
        item.setProductId(2L);
        item.setProductName("Test Product 2");
        item.setQuantity(1);
        item.setUnitPrice(new BigDecimal("50.00"));
        item.setTotalPrice(new BigDecimal("50.00"));
        secondOrder.setItems(Arrays.asList(item));

        mockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testOrder)))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(secondOrder)))
                .andExpect(status().isOk());

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testMinimalOrder() throws Exception {
        Order minimalOrder = new Order();
        minimalOrder.setOrderId("TEST-003");
        minimalOrder.setCustomer(testUser);
        minimalOrder.setStatus("PENDING");

        mockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalOrder)))
                .andExpect(status().isOk());

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }
} 