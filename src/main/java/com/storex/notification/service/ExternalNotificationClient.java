package com.storex.notification.service;

import com.storex.notification.model.NotificationChannel;
import com.storex.notification.model.NotificationRequest;
import com.storex.notification.model.OrderCreatedEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
public class ExternalNotificationClient {
    private final WebClient webClient;

    public ExternalNotificationClient(@Qualifier("notificationWebClient") WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<Void> send(NotificationChannel channel, OrderCreatedEvent event) {
        String path = channel == NotificationChannel.ZALO
                ? "/api/notify/zalo"
                : "/api/notify/email";

        return webClient.post()
                .uri(path)
                .bodyValue(NotificationRequest.from(event))
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(3))
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1))
                        .filter(WebClientRetrySupport::isRetryable))
                .then();
    }
}

