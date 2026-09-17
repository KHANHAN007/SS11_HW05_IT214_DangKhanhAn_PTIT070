package com.storex.notification.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class WebClientConfig {
    private static final int TIMEOUT_SECONDS = 3;

    @Bean
    HttpClient externalApiHttpClient() {
        return HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, TIMEOUT_SECONDS * 1_000)
                .responseTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .doOnConnected(connection -> connection.addHandlerLast(
                        new ReadTimeoutHandler(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
    }

    @Bean
    @Qualifier("preferenceWebClient")
    WebClient preferenceWebClient(WebClient.Builder builder, HttpClient externalApiHttpClient) {
        return builder
                .baseUrl("http://localhost:8081")
                .clientConnector(new ReactorClientHttpConnector(externalApiHttpClient))
                .build();
    }

    @Bean
    @Qualifier("notificationWebClient")
    WebClient notificationWebClient(WebClient.Builder builder, HttpClient externalApiHttpClient) {
        return builder
                .baseUrl("http://localhost:8082")
                .clientConnector(new ReactorClientHttpConnector(externalApiHttpClient))
                .build();
    }
}

