package com.storex.notification.model;

public record NotificationRequest(String orderId, String userId, String message) {
    public static NotificationRequest from(OrderCreatedEvent event) {
        return new NotificationRequest(event.orderId(), event.userId(), event.message());
    }
}

