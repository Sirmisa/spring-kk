package com.example.demo.misc;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${kafka.ssl.keystore}")
    private String keyStorePath;

    @Value("${kafka.ssl.truststore}")
    private String trustStorePath;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "your-bootstrap-server:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // SSL Configuration
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "your-keystore-password");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "your-truststore-password");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "your-key-password");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
