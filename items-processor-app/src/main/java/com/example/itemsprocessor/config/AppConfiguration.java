package com.example.itemsprocessor.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfiguration {
    @org.springframework.context.annotation.Bean
    WebClient webClient(WebClient.Builder builder) { return builder.build(); }
}
