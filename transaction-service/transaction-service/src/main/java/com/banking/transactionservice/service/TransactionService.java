package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction-initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction-completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction-refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud-detected";


    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * SAGA STEP - 1: Initiate transfer
     * Deducts from sender via feign
     * Saves transaction as PROCESSING
     * Publish event to kafka for fraud check
     * Returns.
     *
     * @return
     * @Param request
     */

    public TransactionResponse transfer(TransferRequest transferRequest) {

        log.info("SAGA START - Transfer: {} -> {} amount : {}",
                transferRequest.getSenderAccountNumber(), transferRequest.getRecipientAccountNumber(), transferRequest.getAmount());

        // SAGA STEP 1: Deduct from sender
        log.info("calling account service client ...");
        accountServiceClient.deductBalance(transferRequest.getSenderAccountNumber(), transferRequest.getAmount());
        log.info("account service client called successfully");

        Transaction transaction = Transaction.builder()
                .senderAccountNumber(transferRequest.getSenderAccountNumber())
                .receiverAccountNumber(transferRequest.getRecipientAccountNumber())
                .amount(transferRequest.getAmount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(transferRequest.getDescription())
                .referenceNumber(UUID.randomUUID().toString())
                .build();
        TransactionResponse.builder().build();

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved with id: {}", savedTransaction.getId());

        // publishing event to kafka for fraud check
        TransactionInitiatedEvent event = TransactionInitiatedEvent.builder()
                .transactionId(savedTransaction.getId())
                .senderAccountNumber(savedTransaction.getSenderAccountNumber())
                .receiverAccountNumber(savedTransaction.getReceiverAccountNumber())
                .amount(savedTransaction.getAmount())
                .description(savedTransaction.getDescription())
                .build();
        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("Transaction initiated event published to Kafka");
        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        log.info("fetching transaction id : {}", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));
        return mapToResponse(transaction);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        log.info("fetching transaction history for account: {}", accountNumber);
        // Implementation for fetching transaction history
        List<Transaction> transactions = transactionRepository.
                findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber);
        return transactions.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("verifying transaction id : {}, otp : {}", transactionId, otp);

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));

        String otpKey = "verification.otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if (storedOtp == null) {
            log.warn("OTP expired for transaction {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if (!otp.equals(storedOtp)) {
            log.warn("OTP mismatch - blocking account and refunding amount ", transactionId);
            redisTemplate.delete(otpKey);
            blockAndCompensateTransaction(transaction, "OTP mismatch - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction
        log.info("OTP verified for transaction {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);

    }

    public void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {}: amount {}",
                transaction.getSenderAccountNumber(), transaction.getAmount());

        // CREDIT Money back to sender synchronously
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(), transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason
                + "- SAGA Compensation executed, amount refunded at " + LocalDateTime.now()
        );

        transactionRepository.save(transaction);

        //Publish refund event - Notification service will alert user
        Map<String, Object> refundEvent = new HashMap<>();

        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);
        refundEvent.put("accountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("transactionId", transaction.getId());

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,
                transaction.getId(), refundEvent);
        log.info("SAGA COMPENSATION - refunding: {} amount {}",
                transaction.getSenderAccountNumber(), transaction.getAmount());

    }

    public void blockAndCompensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - blocking account and refunding: {}: amount {}",
                transaction.getSenderAccountNumber(), transaction.getAmount());

        Map<String, Object> fraudDetectEvent = new HashMap<>();
        fraudDetectEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudDetectEvent.put("transactionId", transaction.getId());
        fraudDetectEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudDetectEvent);

        log.warn("fraud.detected published - account {} will be blocked, kindly visit nearest bank branch",
                transaction.getSenderAccountNumber());

        // SAGA COMEPENSATION - Refund amount to sender
        compensateTransaction(transaction, reason);

    }

    public void completeTransaction(Transaction transaction) {

        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompleted(LocalDateTime.now());
        transactionRepository.save(transaction);

        log.info("Transaction {} completed successfully", transaction.getId());

        TransactionCompletedEvent event = TransactionCompletedEvent.builder()
                .transactionId(transaction.getId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .amount(transaction.getAmount())
                .description(transaction.getDescription())
                .build();

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), event);
        log.info("Transaction {} completed successfully", transaction.getId());
    }

    public void processCleanFraudCheckResult(String transactionId) {

        Transaction transaction = transactionRepository.findById(transactionId).
                orElseThrow(() -> new RuntimeException("Transaction not found for id: " + transactionId));

        if (transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.info("Transaction {} is not in processing status", transactionId);
            return;
        }
        completeTransaction(transaction);
    }

    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        return TransactionResponse.builder()
                .id(savedTransaction.getId())
                .senderAccountNumber(savedTransaction.getSenderAccountNumber())
                .receiverAccountNumber(savedTransaction.getReceiverAccountNumber())
                .amount(savedTransaction.getAmount())
                .status(savedTransaction.getStatus())
                .description(savedTransaction.getDescription())
                .failureReason(savedTransaction.getFailureReason())
                .referenceNumber(savedTransaction.getReferenceNumber())
                .build();
    }
}
