package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class AccountEventConsumer {
    private final AccountService accountService;

    @KafkaListener(topics = "transaction.completed", groupId = "account-service-group")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
        try {

            String receiverAccountNumber = payload.get("receiverAccountNumber").toString();
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Consuming transaction completed event for receiver: {} with amount: {}", receiverAccountNumber, amount);

            accountService.creditBalance(receiverAccountNumber, amount);

        } catch (Exception e) {
            log.error("Error processing transaction completed event: {}", e.getMessage(), e);

        }
    }

    @KafkaListener(topics = "fraud.detected", groupId = "account-service-group")
    public void consumeFraudDetected(@Payload Map<String, Object> payload) {
        try {
            // Extract the account number and reason from the event payload
            String accountNumber = payload.get("accountNumber").toString();
            String reason = payload.get("reason") != null ? payload.get("reason").toString() : "Suspicious activity detected";

            log.warn("Fraud detected event received for account: {}. Reason: {}", accountNumber, reason);

            // Call the account service method to freeze/block the account
            accountService.blockAccount(accountNumber);

            log.info("Successfully blocked account due to fraud alert: {}", accountNumber);

        } catch (Exception e) {
            log.error("Error processing fraud detected event: {}", e.getMessage(), e);
            // In a production environment, send this message to a Dead Letter Queue (DLQ)

        }
    }


}
