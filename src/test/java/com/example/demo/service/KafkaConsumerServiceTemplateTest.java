package com.example.demo.service;

import com.example.demo.config.KafkaConfig;
import com.example.demo.model.Customer;
import com.example.demo.model.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=test-group",
    "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
    "spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.demo.model",
    "spring.main.allow-bean-definition-overriding=true"
})
@EmbeddedKafka(partitions = 1,
               brokerProperties = {"listeners=PLAINTEXT://localhost:9000", "port=9000"},
               topics = {"orders"},
               ports = 9000)
@Import(KafkaConfig.class)
@DirtiesContext
class KafkaConsumerServiceTemplateTest {

    private KafkaTemplate<String, Order> orderKafkaTemplate;

    @SpyBean
    private KafkaConsumerService consumerService;

    private Order testOrder;

    @BeforeEach
    void setup() {
        // Setup Kafka templates
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Order> orderProducerFactory = 
            new DefaultKafkaProducerFactory<>(producerProps);

        orderKafkaTemplate = new KafkaTemplate<>(orderProducerFactory);

        // Setup test customer
        Customer testCustomer = new Customer(1L, "Test Customer", "test@example.com");

        // Setup test order
        testOrder = new Order();
        testOrder.setOrderId("TEST-001");
        testOrder.setCustomer(testCustomer);
        testOrder.setTotalAmount(BigDecimal.valueOf(100.00));
        testOrder.setStatus("PENDING");
    }

    @Test
    void testConsumeOrder() {
        orderKafkaTemplate.send("orders", testOrder.getOrderId(), testOrder);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(Order.class));
            });
    }

} 