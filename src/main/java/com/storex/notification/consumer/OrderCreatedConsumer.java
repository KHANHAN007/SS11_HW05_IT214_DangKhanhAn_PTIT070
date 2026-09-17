package com.storex.notification.consumer;

import com.storex.notification.model.OrderCreatedEvent;
import com.storex.notification.model.ProcessingResult;
import com.storex.notification.service.IdempotencyService;
import com.storex.notification.service.NotificationWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderCreatedConsumer.class);

    private final NotificationWorkflow workflow;
    private final IdempotencyService idempotencyService;

    public OrderCreatedConsumer(
            NotificationWorkflow workflow,
            IdempotencyService idempotencyService) {
        this.workflow = workflow;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(topics = "${storex.kafka.order-topic}")
    public void consume(OrderCreatedEvent event) {
        if (!idempotencyService.tryStart(event.orderId())) {
            log.info("Bỏ qua order {} vì đã hoặc đang được xử lý", event.orderId());
            return;
        }

        workflow.process(event).subscribe(
                result -> finish(event.orderId(), result),
                error -> {
                    idempotencyService.release(event.orderId());
                    log.error("Lỗi ngoài dự kiến khi xử lý order {}: {}",
                            event.orderId(), error.getMessage());
                });
    }

    private void finish(String orderId, ProcessingResult result) {
        if (result == ProcessingResult.SENT) {
            idempotencyService.markProcessed(orderId);
            return;
        }
        idempotencyService.release(orderId);
    }
}

