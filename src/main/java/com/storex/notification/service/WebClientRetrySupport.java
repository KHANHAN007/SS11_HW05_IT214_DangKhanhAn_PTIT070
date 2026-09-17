package com.storex.notification.service;

import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.concurrent.TimeoutException;

final class WebClientRetrySupport {
    private WebClientRetrySupport() {
    }

    static boolean isRetryable(Throwable error) {
        if (error instanceof TimeoutException || error instanceof WebClientRequestException) {
            return true;
        }
        return error instanceof WebClientResponseException responseException
                && responseException.getStatusCode().is5xxServerError();
    }
}

