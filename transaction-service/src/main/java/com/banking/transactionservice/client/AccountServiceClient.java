package com.banking.transactionservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

@FeignClient(name = "account-service", path = "/api/v1/accounts/")
public interface AccountServiceClient {
    @PutMapping("{accountNumber}/deduct")
    String deductBalance(@PathVariable String accountNumber,
                         @RequestParam BigDecimal amount);


    @PutMapping("{accountNumber}/credit")
    String creditBalance(@PathVariable String accountNumber,
                         @RequestParam BigDecimal amount);

    @GetMapping("{accountNumber}/balance")
    BigDecimal getBalance(@PathVariable String accountNumber);
}
