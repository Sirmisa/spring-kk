package com.example.demo;

import com.example.demo.config.KafkaTestConfig;
import com.example.demo.model.User;
import com.example.demo.model.Order;
import com.example.demo.model.OrderItem;
import com.example.demo.model.Address;
import com.example.demo.service.KafkaConsumerService;
import com.example.demo.service.KafkaProducerService;
import com.example.demo.controller.UserController;
import com.example.demo.controller.OrderController;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.http.MediaType;

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
               topics = {"users", "orders"})
@Import(KafkaTestConfig.class)
@DirtiesContext
@TestPropertySource(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.main.allow-bean-definition-overriding=true"
})
class KafkaIntegrationTest {

    @Autowired
    private KafkaProducerService producerService;

    @SpyBean
    private KafkaConsumerService consumerService;

    @Autowired
    private UserController userController;

    @Autowired
    private OrderController orderController;

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc userMockMvc;
    private MockMvc orderMockMvc;
    private User testUser;
    private Order testOrder;

    @BeforeEach
    void setup() {
        userMockMvc = MockMvcBuilders.standaloneSetup(userController).build();
        orderMockMvc = MockMvcBuilders.standaloneSetup(orderController).build();

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
        
        OrderItem item1 = new OrderItem();
        item1.setProductId(1L);
        item1.setProductName("Test Product 1");
        item1.setQuantity(2);
        item1.setUnitPrice(new BigDecimal("49.99"));
        item1.setTotalPrice(new BigDecimal("99.98"));

        OrderItem item2 = new OrderItem();
        item2.setProductId(2L);
        item2.setProductName("Test Product 2");
        item2.setQuantity(1);
        item2.setUnitPrice(new BigDecimal("60.00"));
        item2.setTotalPrice(new BigDecimal("60.00"));

        testOrder.setItems(Arrays.asList(item1, item2));
        testOrder.setTotalAmount(new BigDecimal("159.98"));
        testOrder.setStatus("PENDING");
    }

    @Test
    void testUserFlow() throws Exception {
        // Send POST request to controller
        userMockMvc.perform(post("/api/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isOk());

        // Verify the consumer received the message
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
            });
    }

    @Test
    void testOrderFlow() throws Exception {
        // Send POST request to controller
        orderMockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testOrder)))
                .andExpect(status().isOk());

        // Verify the consumer received the message
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testDirectProducerConsumer() {
        // Use producer service directly
        producerService.sendMessage("users", testUser.getId().toString(), testUser);

        // Verify consumer received the message
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
            });
    }

    @Test
    void testMultipleUserMessages() throws Exception {
        // Create second test user
        User secondUser = new User(2L, "Second User", "second@example.com");

        // Send first user
        userMockMvc.perform(post("/api/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isOk());

        // Send second user
        userMockMvc.perform(post("/api/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(secondUser)))
                .andExpect(status().isOk());

        // Verify both messages were consumed
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeUser(any(User.class));
            });
    }

    @Test
    void testMultipleOrderMessages() throws Exception {
        // Create second test order
        Order secondOrder = new Order();
        secondOrder.setOrderId("TEST-002");
        secondOrder.setOrderDate(LocalDateTime.now());
        secondOrder.setCustomer(testUser);
        secondOrder.setStatus("PENDING");
        secondOrder.setTotalAmount(new BigDecimal("50.00"));
        
        OrderItem item = new OrderItem();
        item.setProductId(3L);
        item.setProductName("Test Product 3");
        item.setQuantity(1);
        item.setUnitPrice(new BigDecimal("50.00"));
        item.setTotalPrice(new BigDecimal("50.00"));
        secondOrder.setItems(Arrays.asList(item));

        // Send first order
        orderMockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testOrder)))
                .andExpect(status().isOk());

        // Send second order
        orderMockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(secondOrder)))
                .andExpect(status().isOk());

        // Verify both messages were consumed
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testMixedMessageTypes() throws Exception {
        // Send user message
        userMockMvc.perform(post("/api/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isOk());

        // Send order message
        orderMockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testOrder)))
                .andExpect(status().isOk());

        // Verify both types of messages were consumed
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

    @Test
    void testDirectMultipleMessages() {
        // Create second test user
        User secondUser = new User(2L, "Second User", "second@example.com");

        // Send messages directly through producer
        producerService.sendMessage("users", testUser.getId().toString(), testUser);
        producerService.sendMessage("users", secondUser.getId().toString(), secondUser);

        // Verify both messages were consumed
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeUser(any(User.class));
            });
    }

    @Test
    void testOrderWithNullFields() throws Exception {
        // Create order with minimal required fields
        Order minimalOrder = new Order();
        minimalOrder.setOrderId("TEST-003");
        minimalOrder.setCustomer(testUser);
        minimalOrder.setStatus("PENDING");

        // Send order
        orderMockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalOrder)))
                .andExpect(status().isOk());

        // Verify message was consumed
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }
} 