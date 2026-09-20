package com.banking.paymentservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PaymentOrderResponse {

    private String paymentId;

    private String razerpayOrderId;

    private String accountNumber;

    private BigDecimal amount;

    private String currency;

    private String status;

    private String razerpayKeyId;
}
