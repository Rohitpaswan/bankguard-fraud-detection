package com.banking.frauddetectionservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


import java.math.BigDecimal;
import java.util.Map;

@RequiredArgsConstructor
@Slf4j
@Component
public class TransactionEventConsumer {
    private final FraudDetectionService fraudDetectionService;

    /**
     * Listens to transaction.initiated topic.
     * Every transaction goes through fraud check be fore completing.
     */
    @KafkaListener(
            topics = "transaction.initiated",
            groupId = "fraud-detection-group"
    )
    public void consumeTransactionInitiated(
            @Payload Map<String, Object> payload) {
        log.info("Received transaction for fraud check: {}",
                payload.get("transactionId"));

        try {
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception e) {
            log.error("Error in fraud detection: {}", e.getMessage());
        }
    }

    /**
     * Listens to transaction.completed topic.
     * Every transaction goes through fraud check be fore completing.
     */


    @KafkaListener(
            topics = "transaction.completed",
            groupId = "fraud-detection-group"
    )
    public void consumeOnTransactionComplete(@Payload Map<String, Object> payload) {
        fraudDetectionService.recordCompleteTransaction(
                (String) payload.get("transactionId"),
                (String) payload.get("senderAccountNumber"),
                new BigDecimal(String.valueOf(payload.get("amount")))
        );
    }



}
