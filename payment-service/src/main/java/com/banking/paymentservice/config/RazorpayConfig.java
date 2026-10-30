package com.banking.paymentservice.config;


import com.razorpay.RazorpayClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RazorpayConfig {

    @Value("${razorpay.key.id}")
    private String razorpayApiKey;

    @Value("${razorpay.secret}")
    private String razorpayApiSecret;

    @Value("${razorpay.webhook.secret}")
    private String razorpayWebhook;

    @Bean
    public RazorpayClient razorpayClient() throws Exception {
        return new RazorpayClient(razorpayApiKey, razorpayApiSecret);
    }

    public String getWebhookSecret() {
        return razorpayWebhook;
    }
}
