package com.banking.paymentservice.service;


import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    private final KafkaTemplate<String, Object> paymentKafkaTemplate;

    private final PaymentRepository paymentRepository;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    /***
     * Create Razorpay payment order
     *
     * FLOW:
     * 1.Create order in razerpay
     * 2.Save Payment record in DB
     * 3.Return order details to frontend
     * 4.Frontend shows Razerpay Checkout
     * 5.User pays
     * 6.Razorpay calls webhook
     * @param request
     * @return
     */
    public PaymentOrderResponse createPaymentOrder(
            CreatePaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account:{} amount:{}",
                request.getAccountNumber(), request.getAmount());

        RazorpayClient razerpayClient = new RazorpayClient(keyId, keySecret);
        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", request.getAmount().multiply(new BigDecimal(100))); //
        orderRequest.put("currency", "INR");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace("-", "").substring(0, 10)
        );

        Order order = razerpayClient.orders.create(orderRequest);
        log.info("Payment order created: {}", order.get("id").toString());

        // Save payment record
        Payment payment = Payment.builder()
                .razerpayOrderId(order.get("id").toString())
                .accountNumber(request.getAccountNumber())
                .amount(request.getAmount())
                .currency("INR")
                .status(PaymentStatus.CREATED)
                .description(request.getDescription())
                .build();

        Payment savedPayment = paymentRepository.save(payment);

        return mapToResponse(savedPayment);
    }

    public void handleWebhook(Map<String, Object> payload) {

        log.info("Received Razorpay Webhook: {}", payload.get("event"));

        String event = (String) payload.get("event");

        if ("payment.captured".equals(event)) {
            handlePaymentSuccess(payload);
        } else if ("payment.failed".equals(event)) {
            handlePaymentFailue(payload);
        }

    }

    private void handlePaymentSuccess(Map<String, Object> payload) {

        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazerpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found"));

            payment.setRazerpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("paymentStatus", PaymentStatus.COMPLETED);
            event.put("amount", payment.getAmount().multiply(new BigDecimal(100)));
            event.put("accountNumber", payment.getAccountNumber());
            event.put("razerpayPaymentId", payment.getRazerpayOrderId());

            paymentKafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
            log.info("Payment completed: {}", payment.getId());

        } catch (Exception e) {
            log.info("Payment failed: {}", e.getMessage());
        }
    }

    private void handlePaymentFailue(Map<String, Object> payload) {

        try {

            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazerpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found"));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay API");
            paymentRepository.save(payment);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("paymentStatus", PaymentStatus.FAILED);
            event.put("amount", payment.getAmount().multiply(new BigDecimal(100)));
            event.put("accountNumber", payment.getAccountNumber());
            event.put("reason", "Payment failed via Razorpay API");

            paymentKafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);
            log.warn("Payment failed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Payment failed: {}", e.getMessage());
        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {

        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }

    private PaymentOrderResponse mapToResponse(Payment savedPayment) {

        return PaymentOrderResponse.builder()
                .paymentId(savedPayment.getId().toString())
                .accountNumber(savedPayment.getAccountNumber())
                .amount(savedPayment.getAmount())
                .currency(savedPayment.getCurrency())
                .razerpayOrderId(savedPayment.getRazerpayOrderId())
                .status(savedPayment.getStatus().toString())
                .razerpayKeyId(keyId)
                .build();
    }


}
