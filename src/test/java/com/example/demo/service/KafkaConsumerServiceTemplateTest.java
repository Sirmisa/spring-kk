package com.example.demo.service;

import com.example.demo.config.KafkaTestConfig;
import com.example.demo.model.User;
import com.example.demo.model.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
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
public class KafkaConsumerServiceTemplateTest {

    private KafkaTemplate<String, User> userKafkaTemplate;
    private KafkaTemplate<String, Order> orderKafkaTemplate;

    @SpyBean
    private KafkaConsumerService consumerService;

    private User testUser;
    private Order testOrder;

    @Before
    public void setup() {
        // Setup Kafka templates
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, User> userProducerFactory = 
            new DefaultKafkaProducerFactory<>(producerProps);
        DefaultKafkaProducerFactory<String, Order> orderProducerFactory = 
            new DefaultKafkaProducerFactory<>(producerProps);

        userKafkaTemplate = new KafkaTemplate<>(userProducerFactory);
        orderKafkaTemplate = new KafkaTemplate<>(orderProducerFactory);

        // Setup test user
        testUser = new User(1L, "Test User", "test@example.com");

        // Setup test order
        testOrder = new Order();
        testOrder.setOrderId("TEST-001");
        testOrder.setCustomer(testUser);
        testOrder.setStatus("PENDING");
    }

    @Test
    public void testConsumeUser() {
        userKafkaTemplate.send("users", testUser.getId().toString(), testUser);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
            });
    }

    @Test
    public void testConsumeOrder() {
        orderKafkaTemplate.send("orders", testOrder.getOrderId(), testOrder);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

    @Test
    public void testConsumeMultipleUsers() {
        User secondUser = new User(2L, "Second User", "second@example.com");

        userKafkaTemplate.send("users", testUser.getId().toString(), testUser);
        userKafkaTemplate.send("users", secondUser.getId().toString(), secondUser);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeUser(any(User.class));
            });
    }
} 