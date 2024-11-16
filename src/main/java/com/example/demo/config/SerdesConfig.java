package com.example.demo.config;

import com.example.demo.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class SerdesConfig {
    
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
    
    @Bean
    public Serde<Order> orderSerde(ObjectMapper mapper) {
        JsonSerializer<Order> serializer = new JsonSerializer<>(mapper);
        JsonDeserializer<Order> deserializer = new JsonDeserializer<>(Order.class, mapper);
        deserializer.addTrustedPackages("com.example.demo.model");
        
        Serde<Order> serde = Serdes.serdeFrom(serializer, deserializer);
        log.info("Created Order Serde with JsonSerializer and JsonDeserializer");
        return serde;
    }
} 