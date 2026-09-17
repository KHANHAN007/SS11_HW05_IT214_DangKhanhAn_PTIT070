package com.storex.notification.service;

import com.storex.notification.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class DlqPublisher {
    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
    private static final String DLQ_TOPIC = "storex-order-events.DLQ";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public DlqPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<Void> publish(OrderCreatedEvent event, Throwable cause) {
        return Mono.fromFuture(kafkaTemplate.send(DLQ_TOPIC, event.orderId(), event))
                .doOnSuccess(result -> log.error(
                        "Đã đẩy order {} vào DLQ do lỗi gửi thông báo", event.orderId(), cause))
                .doOnError(error -> log.error(
                        "Không thể đẩy order {} vào DLQ: {}", event.orderId(), error.getMessage()))
                .then();
    }
}

