package com.example.demo.service;

import com.example.demo.config.KafkaConfig;
import com.example.demo.proto.AddressProto;
import com.example.demo.proto.CustomerProto;
import com.example.demo.proto.OrderItemProto;
import com.example.demo.proto.OrderProto;
import com.example.demo.kafka.ProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import com.example.demo.kafka.ProtobufSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.LocalDateTime;
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
    "spring.main.allow-bean-definition-overriding=true"
})
@EmbeddedKafka(partitions = 1,
               brokerProperties = {"listeners=PLAINTEXT://localhost:9000", "port=9000"},
               topics = {"orders"},
               ports = 9000)
@Import(KafkaConfig.class)
@DirtiesContext
class KafkaConsumerServiceTemplateTest {

    private KafkaTemplate<String, OrderProto> orderKafkaTemplate;

    @SpyBean
    private KafkaConsumerService consumerService;

    private OrderProto testOrder;

    @BeforeEach
    void setup() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtobufSerializer.class);

        DefaultKafkaProducerFactory<String, OrderProto> orderProducerFactory = 
            new DefaultKafkaProducerFactory<>(producerProps);
        orderProducerFactory.setValueSerializer(new ProtobufSerializer<>());

        orderKafkaTemplate = new KafkaTemplate<>(orderProducerFactory);

        CustomerProto testCustomer = CustomerProto.newBuilder()
            .setId(1L)
            .setName("Test Customer")
            .setEmail("test@example.com")
            .build();

        OrderItemProto testItem = OrderItemProto.newBuilder()
            .setProductId(1L)
            .setProductName("Test Product")
            .setQuantity(1)
            .setUnitPrice(100.00)
            .setTotalPrice(100.00)
            .build();

        testOrder = OrderProto.newBuilder()
            .setOrderId("TEST-001")
            .setOrderDate(LocalDateTime.now().toString())
            .setCustomer(testCustomer)
            .addItems(testItem)
            .setTotalAmount(100.00)
            .setStatus("PENDING")
            .build();
    }

    @Test
    void testConsumeOrder() {
        orderKafkaTemplate.send("orders", testOrder.getOrderId(), testOrder);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeOrder(any(OrderProto.class));
            });
    }
} 