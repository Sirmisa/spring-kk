package com.example.demo.config;

import com.example.demo.kafka.GenericJsonDeserializer;
import com.example.demo.model.Customer;
import com.example.demo.model.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration class that sets up producers and consumers with JSON serialization.
 * Configures both generic producer factory and type-specific consumer factory.
 */
@Configuration
@Slf4j
public class KafkaConfig {

    //@Value("${spring.kafka.bootstrap-servers}")
    @Value("localhost:9000")
    private String bootstrapServers;

    /**
     * Creates a consumer factory specifically for Order messages.
     * Configures JSON deserialization for Order class.
     */
    @Bean
    public ConsumerFactory<String, Order> orderConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericJsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        log.info("Configuring order consumer factory with group: order-group");
        return new DefaultKafkaConsumerFactory<>(
            config,
            new StringDeserializer(),
            new GenericJsonDeserializer<>(Order.class)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory());
        log.info("Created Kafka listener container factory for Order messages");
        return factory;
    }
} 