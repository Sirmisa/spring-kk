package com.example.demo.stream;

import com.example.demo.model.User;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.common.serialization.Serdes;
import com.example.demo.config.UserSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Kafka Streams processor for User events
 * Consumes user messages and logs them for further use
 */
@Component
public class UserStreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(UserStreamProcessor.class);

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        logger.info("Initializing Kafka Streams pipeline for user processing");

        // Create a stream from the input topic with explicit serde
        KStream<String, User> userStream = streamsBuilder.stream("users-input", 
            Consumed.with(Serdes.String(), new UserSerde()));
        
        // Just consume and log the users
        userStream.foreach((key, user) -> {
            logger.info("Consumed user - Key: {}, User ID: {}", key, user.getId());
            logger.debug("User details - Name: {}, Email: {}", 
                user.getName(), 
                user.getEmail());
            // The user object is now available for any other processing needed
        });

        logger.info("Kafka Streams pipeline built successfully");
    }
} 