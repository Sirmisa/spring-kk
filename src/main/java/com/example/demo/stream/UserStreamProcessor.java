package com.example.demo.stream;

import com.example.demo.model.User;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Kafka Streams processor for User events
 * Processes user messages from input topic and forwards them to output topic
 */
@Component
public class UserStreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(UserStreamProcessor.class);

    /**
     * Builds the Kafka Streams processing pipeline
     * @param streamsBuilder The streams builder instance
     */
    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        logger.info("Initializing Kafka Streams pipeline for user processing");

        // Create a stream from the input topic
        KStream<String, User> userStream = streamsBuilder.stream("users-input");
        logger.debug("Created Kafka Stream for 'users-input' topic");

        // Process and log the user data
        userStream
            .peek((key, user) -> {
                logger.info("Processing user event - Key: {}, User: {}", key, user);
                logger.debug("User details - ID: {}, Name: {}, Email: {}", 
                    user.getId(), user.getName(), user.getEmail());
            })
            // Forward to output topic
            .peek((key, user) -> logger.debug("Forwarding user to output topic - Key: {}", key))
            .to("users-output");

        logger.info("Kafka Streams pipeline built successfully");
    }
} 