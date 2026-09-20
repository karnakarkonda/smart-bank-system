package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class FraudDetectionService {

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_RESULT_CLEAN_TOPIC = "fraud.check.result.clean";

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    public void checkTransaction(Map<String, Object> event) {
        if (event == null || event.isEmpty()) {
            log.warn("Received empty fraud check event");
            return;
        }

        String transactionId = (String) event.get("transactionId");
        String accountNumber = (String) event.getOrDefault("senderAccountNumber", event.get("accountNumber"));
        String receiverAccountNumber = (String) event.getOrDefault("receiverAccountNumber", event.get("recipientAccountNumber"));
        BigDecimal amount = (BigDecimal) event.get("amount");

        if (transactionId == null || accountNumber == null || amount == null) {
            log.warn("Incomplete transaction event received: {}", event);
            return;
        }

        if (transactionId.isBlank() || accountNumber.isBlank()) {
            log.warn("Incomplete transaction event received: {}", event);
            return;
        }

        log.info("Checking transaction {} for fraud: sender={},amount={}",
                transactionId, accountNumber, amount);

        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);
        if (senderBalance == null) {
            log.warn("Could not fetch sender balance for account {}", accountNumber);
            return;
        }

        FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBalance);

        if (result.isFraud()) {
            log.info("Suspicious transaction detected - account {}" +
                    "reason: {} - requesting OTP verification", accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
        } else {
            // transaction is clean
            log.info("Transaction {} passed fraud checks", transactionId);

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_RESULT_CLEAN_TOPIC, transactionId, transactionCleanEvent);
        }

    }

    private FraudCheckResult performFraudChecks(
            String accountNumber,
            BigDecimal amount,
            BigDecimal senderBalance) {

        // Pattern 1: Velocity Check
        if (isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(true, "Too many transactions in 60 sec " + " - Velocity limit exceeded");
        }

        // Pattern 2: Account check
        if (isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(true, "unusal transaction amount - " + "Suspicious transaction amount detected");
        }

        // Pattern 3: Balance Check
        if (senderBalance.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBalance, amount)) {
            return new FraudCheckResult(true, "transaction exceeds 90% of account balance - " + "Balance check failed");
        }
        // no fraud found
        return new FraudCheckResult(false, null);
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        // Implementation for amount suspicious check
        String avgKey = "fraud.avg_amount" + accountNumber;   //fraud.avg-amount123
        String avgStr = redisTemplate.opsForValue().get(avgKey); // first null

        if (avgStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toString()); // 2000$
            return true;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(
                BigDecimal.valueOf(suspiciousAmountMultiplier)
        );

        //udpate running average
        BigDecimal newAvg = avgAmount.add(amount)
                .divide(BigDecimal.valueOf(2), BigDecimal.ROUND_HALF_UP);

        redisTemplate.opsForValue().set(avgKey, newAvg.toString());

        log.info("Amount check - account {} amount {} avg {} threshold {} suspicious {}",
                accountNumber, amount, avgAmount, threshold, amount.compareTo(threshold) > 0);

        return amount.compareTo(threshold) > 0;
    }

    private boolean isVelocityExceeded(String accountNumber) {
        // Implementation for velocity check
        String key = "fraud:velocity." + accountNumber;
        Long count = Long.valueOf(redisTemplate.opsForValue().get(key));

        if (count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account {} count {}/{}",
                accountNumber, count, maxTransactionPerMinute);

        return count != null && count > maxTransactionPerMinute;
    }

    private boolean isBalanceCheckFailed(
            BigDecimal senderBalance, BigDecimal amount) {

        BigDecimal maxAllowed = senderBalance.multiply(
                BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {} maxAllowed: {} suspicious: {}",
                amount, maxAllowed, amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }

}
