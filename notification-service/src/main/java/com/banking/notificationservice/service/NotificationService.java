package com.banking.notificationservice.service;

import com.banking.notificationservice.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = Constant.TRANSACTION_COMPLETED, groupId = "transaction-group")
    public void consumeTransactionCompleted(Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload
                    .get(Constant.SENDER_ACCOUNT_NUMBER);
            String receiverAccount = (String) payload
                    .get(Constant.RECEIVER_ACCOUNT_NUMBER);
            String amount = payload.get(Constant.AMOUNT).toString();

            if (amount == null || (senderAccount == null && receiverAccount == null)) {
                log.warn("Invalid transaction.completed payload, skipping: {}", payload);
                return;
            }

            if (senderAccount != null) {
                sendAlert(senderAccount, "DEBIT ALERT",
                        String.format("₹%s debited from account %s", amount, senderAccount));
            }
            if (receiverAccount != null) {
                sendAlert(receiverAccount, "CREDIT ALERT",
                        String.format("₹%s credited to account %s", amount, receiverAccount));
            }
        }catch (Exception e) {
            log.error("Error sending transaction notification: {}",
                    e.getMessage());
        }
    }



    /**
     * Payment completed via Razorpay.
     */
    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber, "PAYMENT SUCCESSFUL", String.format(
                            "Payment of ₹%s completed. " +
                                    "Razorpay ID: %s", amount, payload.get("razorpayPaymentId")));

        } catch (Exception e) {
            log.error("Error sending payment notification: {}",
                    e.getMessage());
        }
    }


    /**
     * Payment failed via Razorpay.
     */
    @KafkaListener(topics = Constant.PAYMENT_FAILED)
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get(Constant.SENDER_ACCOUNT_NUMBER);
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber, "❌ PAYMENT FAILED", String.format(
                    "Your payment of ₹%s could not be processed. " +
                            "Please try again or contact support.", amount));

        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}",
                    e.getMessage());
        }

    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "🚨 ACCOUNT BLOCKED", String.format(
                            "Your account %s has been blocked. " +
                                    "Reason: %s. " +
                                    "Please contact your bank immediately.", accountNumber, reason));

        } catch (Exception e) {
            log.error("Error sending fraud alert: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            if (accountNumber == null || otp == null || amount == null) {
                // Never log the payload here: it contains the OTP.
                log.warn("Invalid transaction.otp.generated payload, skipping");
                return;
            }

            sendAlert(accountNumber,
                    "🔐 TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. " +
                                    "Reason: %s. " +
                                    "A transaction of ₹%s is pending verification. " +
                                    "Your OTP is: %s. Valid for 5 minutes. " +
                                    "If this wasn't you — ignore this message. " +
                                    "Transaction will be cancelled and amount refunded automatically.",
                            reason, amount, otp, transactionId, otp));

        } catch (Exception e) {
            log.error("Error sending OTP notification: {}",
                    e.getMessage());
        }
    }


    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload
                    .get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(senderAccount, "💰 REFUND PROCESSED",
                    String.format(
                            "Your transaction of ₹%s was cancelled. " +
                                    "Reason: %s. " +
                                    "₹%s has been refunded to account %s.",
                            amount, reason, amount, senderAccount));

        } catch (Exception e) {
            log.error("Error sending refund notification: {}",
                    e.getMessage());
        }
    }















    private void sendAlert(String accountNumber,
                           String subject,
                           String message) {
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("NOTIFICATION SENT");
        log.info("Account : {}", accountNumber);
        log.info("Subject : {}", subject);
        log.info("Message : {}", message);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

}
