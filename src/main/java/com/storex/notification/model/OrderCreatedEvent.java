package com.storex.notification.model;

public record OrderCreatedEvent(
        String eventType,
        String orderId,
        String userId,
        String message) {
}

