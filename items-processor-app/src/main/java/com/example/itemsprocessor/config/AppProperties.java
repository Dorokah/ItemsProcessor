package com.example.itemsprocessor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("items")
public record AppProperties(
        String requestHandler,
        String consumeTopic,
        String publishTopic,
        String webhookUrl,
        String surnameApiUrl,
        String mode,
        String pokedexFile,
        long surnameIntervalSeconds,
        String tempoUrl,
        String grafanaUrl) {
}
