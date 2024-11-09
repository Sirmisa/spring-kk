package com.example.demo.stream;

import com.example.demo.model.Order;
import com.example.demo.config.OrderSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Kafka Streams processor for Order events
 * Consumes order messages and logs them for further use
 */
@Component
public class OrderStreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(OrderStreamProcessor.class);

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        logger.info("Initializing Kafka Streams pipeline for order processing");

        // Create a stream from the input topic with explicit serde
        KStream<String, Order> orderStream = streamsBuilder.stream("orders-input", 
            Consumed.with(Serdes.String(), new OrderSerde()));
        
        // Just consume and log the orders
        orderStream.foreach((key, order) -> {
            logger.info("Consumed order - Key: {}, OrderId: {}", key, order.getOrderId());
            logger.debug("Order details - Customer: {}, Items: {}, Total: ${}", 
                order.getCustomer().getName(),
                order.getItems().size(),
                order.getTotalAmount());
            // The order object is now available for any other processing needed
        });

        logger.info("Kafka Streams pipeline built successfully");
    }
} 