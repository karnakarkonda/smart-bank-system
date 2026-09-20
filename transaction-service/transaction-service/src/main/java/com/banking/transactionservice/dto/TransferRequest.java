package com.banking.transactionservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransferRequest {

    @NotBlank(message="Sender account number is required")
    private String senderAccountNumber;

    @NotBlank(message="Recipient account number is required")
    private String recipientAccountNumber;

    @NotNull(message="Amount is required")
    @Positive(message="Amount must be greater than zero")
    private BigDecimal amount;

    @NotBlank(message="Description is required")
    private String description;


}
