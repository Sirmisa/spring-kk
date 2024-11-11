package com.example.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    public static final String ORDERS_TOPIC = "orders";
    public static final String USERS_TOPIC = "users";

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ORDERS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic usersTopic() {
        return TopicBuilder.name(USERS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
} 