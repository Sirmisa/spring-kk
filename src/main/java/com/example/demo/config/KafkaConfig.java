package com.example.demo.config;

import com.example.demo.kafka.GenericJsonDeserializer;
import com.example.demo.kafka.GenericJsonSerializer;
import com.example.demo.model.User;
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

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Creates a producer factory that can handle any type of message with JSON serialization.
     */
    @Bean
    public <T> ProducerFactory<String, T> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericJsonSerializer.class);
        
        log.info("Configuring producer factory with bootstrap servers: {}", bootstrapServers);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public <T> KafkaTemplate<String, T> kafkaTemplate() {
        log.info("Creating Kafka template");
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Creates a consumer factory specifically for User messages.
     * Configures JSON deserialization for User class.
     */
    @Bean
    public ConsumerFactory<String, User> userConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "user-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericJsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        log.info("Configuring user consumer factory with group: user-group");
        return new DefaultKafkaConsumerFactory<>(
            config,
            new StringDeserializer(),
            new GenericJsonDeserializer<>(User.class)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, User> userKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, User> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(userConsumerFactory());
        log.info("Created Kafka listener container factory for User messages");
        return factory;
    }

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