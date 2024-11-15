package com.example.demo.config;

import com.example.demo.kafka.GenericJsonDeserializer;
import com.example.demo.model.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
public class KafkaTestConfig {

    @Value("${spring.embedded.kafka.brokers}")
    private String bootstrapServers;

    @Bean
    @Primary
    public ConsumerFactory<String, Order> orderConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group-test");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericJsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(
            config,
            new StringDeserializer(),
            new GenericJsonDeserializer<>(Order.class)
        );
    }

    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory());
        return factory;
    }
} 