package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;

import java.util.Map;

public interface PaymentService {
    PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request);
    void handleWebhook(Map<String, Object> payload);
}
