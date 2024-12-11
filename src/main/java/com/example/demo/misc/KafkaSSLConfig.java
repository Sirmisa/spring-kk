package com.example.demo.misc;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

@Configuration
public class KafkaSSLConfig {
    @PostConstruct
    public void copyKeystore() throws IOException {
        Resource resource = new ClassPathResource("test/keystore.jks");
        File keystoreFile = new File("/home/jboss/keystore.jks");
        FileUtils.copyInputStreamToFile(resource.getInputStream(), keystoreFile);
    }
}

