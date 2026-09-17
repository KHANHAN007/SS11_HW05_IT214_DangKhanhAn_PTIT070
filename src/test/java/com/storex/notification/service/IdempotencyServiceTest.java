package com.storex.notification.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotencyServiceTest {
    private final IdempotencyService service = new IdempotencyService();

    @Test
    void skipsDuplicateWhileProcessingAndAfterSuccess() {
        assertTrue(service.tryStart("ORD-123"));
        assertFalse(service.tryStart("ORD-123"));

        service.markProcessed("ORD-123");

        assertTrue(service.isProcessed("ORD-123"));
        assertFalse(service.tryStart("ORD-123"));
    }

    @Test
    void allowsRetryAfterFailedWorkflowIsReleased() {
        assertTrue(service.tryStart("ORD-FAILED"));

        service.release("ORD-FAILED");

        assertTrue(service.tryStart("ORD-FAILED"));
    }
}

