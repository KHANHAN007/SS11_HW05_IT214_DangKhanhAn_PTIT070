package com.storex.notification.service;

import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class IdempotencyService {
    private enum Status { PROCESSING, PROCESSED }

    private final ConcurrentMap<String, Status> orders = new ConcurrentHashMap<>();

    public boolean tryStart(String orderId) {
        return orders.putIfAbsent(orderId, Status.PROCESSING) == null;
    }

    public void markProcessed(String orderId) {
        orders.put(orderId, Status.PROCESSED);
    }

    public void release(String orderId) {
        orders.remove(orderId, Status.PROCESSING);
    }

    public boolean isProcessed(String orderId) {
        return orders.get(orderId) == Status.PROCESSED;
    }
}

