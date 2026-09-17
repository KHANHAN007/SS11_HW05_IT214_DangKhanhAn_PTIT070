package com.storex.notification.service;

import com.storex.notification.model.NotificationChannel;
import com.storex.notification.model.PreferenceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
public class PreferenceClient {
    private static final Logger log = LoggerFactory.getLogger(PreferenceClient.class);
    private final WebClient webClient;

    public PreferenceClient(@Qualifier("preferenceWebClient") WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<NotificationChannel> getPreferredChannel(String userId) {
        return webClient.get()
                .uri("/api/preferences/{userId}", userId)
                .retrieve()
                .bodyToMono(PreferenceResponse.class)
                .map(PreferenceResponse::channel)
                .switchIfEmpty(Mono.error(new IllegalStateException("Preference API trả dữ liệu rỗng")))
                .timeout(Duration.ofSeconds(3))
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1))
                        .filter(WebClientRetrySupport::isRetryable))
                .onErrorResume(error -> {
                    log.warn("Không lấy được kênh của user {}, fallback sang EMAIL: {}",
                            userId, error.getMessage());
                    return Mono.just(NotificationChannel.EMAIL);
                });
    }
}

