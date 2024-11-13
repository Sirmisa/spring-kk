package com.example.demo.service;

import com.example.demo.config.KafkaTestConfig;
import com.example.demo.model.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

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
               topics = {"users"})
@Import(KafkaTestConfig.class)
@DirtiesContext
@TestPropertySource(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.main.allow-bean-definition-overriding=true"
})
class KafkaProducerServiceTest {

    @Autowired
    private KafkaProducerService producerService;

    @SpyBean
    private KafkaConsumerService consumerService;

    private User testUser;

    @BeforeEach
    void setup() {
        testUser = new User(1L, "Test User", "test@example.com");
    }

    @Test
    void testDirectProducerConsumer() {
        producerService.sendMessage("users", testUser.getId().toString(), testUser);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(1)).consumeUser(any(User.class));
            });
    }

    @Test
    void testMultipleMessages() {
        User secondUser = new User(2L, "Second User", "second@example.com");

        producerService.sendMessage("users", testUser.getId().toString(), testUser);
        producerService.sendMessage("users", secondUser.getId().toString(), secondUser);

        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                verify(consumerService, times(2)).consumeUser(any(User.class));
            });
    }
} 