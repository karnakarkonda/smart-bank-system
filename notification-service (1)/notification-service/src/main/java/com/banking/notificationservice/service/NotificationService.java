package com.banking.notificationservice.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "verification.otp.generated")
    public void consumeOTPEvent(
            @Payload Map<String, Object> event
    ) {
        log.info("Consuming OTP event for transaction {}", event.get("transactionId"));

        try {
            String transactionId = (String) event.get("transactionId");
            String otp = (String) event.get("otp");
            String accountNumber = (String) event.get("accountNumber");
            String amount = (String) event.get("amount");
            String reason = (String) event.get("reason");

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account %s for transaction %s of amount %s." +
                                    " Reason: %s. Please verify using OTP: %s",
                            accountNumber, transactionId, amount, reason, otp
                    )
            );
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction-completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> event
    ) {
        log.info("Consuming Transaction Completed for transaction {}", event.get("transactionId"));

        try {
            String senderAccountNumber = (String) event.get("senderAccountNumber");
            String receiverAccountNumber = (String) event.get("receiverAccountNumber");
            String amount = (String) event.get("amount");

            // Debit Alert
            sendAlert(
                    senderAccountNumber,
                    "DEBIT Alert",
                    String.format(
                            "%s debited from account %s", amount, senderAccountNumber
                    )
            );

            // CREDIT Alert
            sendAlert(
                    receiverAccountNumber,
                    "CREDIT Alert",
                    String.format(
                            "%s credited to account %s", amount, receiverAccountNumber
                    )
            );
        } catch (Exception e) {
            log.error("Error consuming Transaction Completed for transaction {}", event.get("transactionId"));
        }

    }

    @KafkaListener(topics = "fraud-detected")
    public void consumeFruadDetected(
            @Payload Map<String, Object> event
    ) {

        try {
            String accountNumber = (String) event.get("accountNumber");
            String reason = (String) event.get("reason");

            sendAlert(
                    accountNumber,
                    "SESPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked." +
                                    "Reason: %s" +
                                    "please contact your bank immediately.",
                            accountNumber, reason

                    )
            );

        } catch (Exception e) {
            log.error("Error consuming Fruad Detected for transaction {}",
                    event.get("transactionId"));
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefund(
            @Payload Map<String, Object> event
    ) {
        try {
            String accountNumber = (String) event.get("accountNumber");
            String amount = (String) event.get("amount");
            String reason = (String) event.get("reason");

            sendAlert(
                    accountNumber,
                    "REFUND PROCESSED",
                    String.format(
                            "your  transaction of %s was cancelled. " +
                                    "Reason: %s" +
                                    "%s has been refunded to account %s",
                            amount, reason, amount, accountNumber
                    )
            );
        } catch (Exception e) {
            log.error("Error consuming TransactionRefund for transaction {}",
                    event.get("transactionId"));
        }
    }


    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> event
    ) {
        try {
            String accountNumber = (String) event.get("accountNumber");
            String amount = (String) event.get("amount");

            sendAlert(
                    accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                            "Payment of % completed" +
                                    "Razorpay ID: %s",
                            amount, (String) event.get("razorpayPaymentId"))
            );
        } catch (Exception e) {
            log.error("Error consuming Payment Completed for transaction {}",
                    event.get("transactionId"));
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> event
    ) {
        try {
            String accountNumber = (String) event.get("accountNumber");
            String amount = (String) event.get("amount");
            String reason = (String) event.get("reason");

            sendAlert(
                    accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of %s could not be processed. " +
                                    "Please try again or contact support.",
                            amount
                    )
            );
        } catch (Exception e) {
            log.error("Error sending payment failure notification",
                   e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {
        // Implementation for sending alert

        log.info("----------------------------------------------------------------------");
        log.info("accountNumber: {}", accountNumber);
        log.info("subject: {}", subject);
        log.info("message: {}", message);
        log.info("----------------------------------------------------------------------");
    }
}
