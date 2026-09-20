package com.banking.accountservice.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;
    /**
     * Consume transaction.completed event from kafka
     * Credits the receiver account with the amount
     * @ param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> transactionCompletedEvent
    ){
        try{
            String receiverAccountNumber = (String) transactionCompletedEvent.get("receiverAccountNumber");
            BigDecimal amount = (BigDecimal) transactionCompletedEvent.get("amount");
            log.info("Crediting account: {} with amount: {}", receiverAccountNumber, amount);
            accountService.creditBalance(receiverAccountNumber, amount);
        } catch (Exception e) {
            log.error("Error occurred while consuming transaction completed event", e);
        }
    }

/**
 * Consume fraud.detected event from kafka
 * Blocks the flagged account
 * @ param payload
 */

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> event){

        try{
            String receiverAccountNumber = (String) event.get("receiverAccountNumber");
            log.info("Blocking account: {} due to fraud detected", receiverAccountNumber);
            accountService.blockAccount(receiverAccountNumber);
        }
        catch (Exception e){
            log.error("Error occurred while consuming fraud detected event", e);
        }
    }
}
