package com.banking.transactionservice.entity;

/**
 * Transaction Lifecycle Flow
 *
 * PENDING -> PROCESSING -> COMPLETED (clean transaction)
 *                       -> PENDING_VERIFICATION (suspicious detected)
 *                                 -> COMPLETED (verified)
 *                                 -> FLAGGED (SAGA REFUND)
 *                       -> FAILED (failed transaction)
 *                       -> FLAGGED
 */

public enum TransactionStatus {

    PENDING,
    COMPLETED,
    PROCESSING,
    PENDING_VERIFICATION,
    FAILED,
    FLAGGED
}
