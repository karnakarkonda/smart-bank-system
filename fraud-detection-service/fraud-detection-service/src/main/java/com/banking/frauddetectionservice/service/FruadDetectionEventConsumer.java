package com.banking.frauddetectionservice.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@AllArgsConstructor
public class FruadDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;

    /**
     * Listens to transaction.initiated topic
     * Every transaction goes through fraud check before completing
     *
     * @param event the transaction initiated event
     */

    @KafkaListener(topics = "transaction-initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiated(
            @Payload Map<String, Object> event
    ) {

        log.info("Received fraud detection for transactionId: {}", event.get("transactionId"));
        try {
            fraudDetectionService.checkTransaction(event);
        } catch (Exception e) {
            log.error("Error occurred while checking transaction for fraud: {}", event.get("transactionId"), e);
        }
    }
}
