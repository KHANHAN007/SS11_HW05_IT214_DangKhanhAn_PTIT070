package com.storex.notification.service;

import com.storex.notification.model.OrderCreatedEvent;
import com.storex.notification.model.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class NotificationWorkflow {
    private static final Logger log = LoggerFactory.getLogger(NotificationWorkflow.class);

    private final PreferenceClient preferenceClient;
    private final ExternalNotificationClient notificationClient;
    private final DlqPublisher dlqPublisher;

    public NotificationWorkflow(
            PreferenceClient preferenceClient,
            ExternalNotificationClient notificationClient,
            DlqPublisher dlqPublisher) {
        this.preferenceClient = preferenceClient;
        this.notificationClient = notificationClient;
        this.dlqPublisher = dlqPublisher;
    }

    public Mono<ProcessingResult> process(OrderCreatedEvent event) {
        return preferenceClient.getPreferredChannel(event.userId())
                .flatMap(channel -> notificationClient.send(channel, event)
                        .doOnSuccess(ignored -> log.info(
                                "Đã gửi thông báo {} cho order {}", channel, event.orderId())))
                .thenReturn(ProcessingResult.SENT)
                .onErrorResume(error -> dlqPublisher.publish(event, error)
                        .thenReturn(ProcessingResult.MOVED_TO_DLQ));
    }
}

