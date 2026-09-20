package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@AllArgsConstructor
@Service
public class TransactionEventConsumer {

    private static final Long OTP_EXPIRE_TIME = 5L;
    private static final String VERIFY_OTP_TOPIC = "verification.otp.generated";

    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionService transactionService;

    /**
     * consume verification.require
     * Generate OTP and ask user to verify
     *
     * @Param payload
     */

    @KafkaListener(topics = "verification.required", groupId = "transaction-service")
    public void consumeVerificationRequired(
            @Payload Map<String, Object> event) {
        log.info("Received verification required event: {}", event);

        // Process the event and perform necessary actions
        try {
            String transactionId = (String) event.get("transactionId");
            String accountNumber = (String) event.get("accountNumber");
            String reason = (String) event.get("reason");

            log.info("Received verification required - transaction : {} reason {}",
                    transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId).
                    orElseThrow(() -> new RuntimeException("Transaction not found for id: " + transactionId));


            if (transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.info("Transaction {} is not in processing status", transactionId);
                return;
            }

            // generate 6 digits OTP
            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

            // Store OTP in Redis - expires in 5 minutes
            String key = "verification.otp" + transactionId;
            redisTemplate.opsForValue().set(key, otp, OTP_EXPIRE_TIME, TimeUnit.MINUTES);

            // Update status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction : {} expires in {} ",
                    transactionId, OTP_EXPIRE_TIME);

            // Notify user
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("otp", otp);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("amount", event.get("amount"));

            // Implementation for notifying user would go here
            kafkaTemplate.send(VERIFY_OTP_TOPIC, transactionId, otpEvent);

            log.info("OTP event sent for verification to topic {} for transaction {}", VERIFY_OTP_TOPIC, transactionId);

        } catch (Exception e) {
            log.error("Error while consuming verification required event", e);
        }
    }

    @KafkaListener(topics="fraud.check.result.clean", groupId = "transaction-service")
    public  void consumeFraudCheckCleanResult(@Payload Map<String, Object> event) {
        log.info("Received fruad clean check event: {}", event);

        try {
            String transactionId = (String) event.get("transactionId");
            transactionService.processCleanFraudCheckResult(transactionId);

        }
        catch (Exception e) {
            log.error("Error while consuming fraud check clean result event", e);
        }
    }
}
