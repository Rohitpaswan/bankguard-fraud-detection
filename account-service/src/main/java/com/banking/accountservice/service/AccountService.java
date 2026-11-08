package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.dto.DeductBalanceResponse;

import java.math.BigDecimal;

public interface AccountService {

    AccountResponse createAccount(CreateAccountRequest request);
    AccountResponse getAccount(String accountNumber);
    BigDecimal getBalance(String accountNumber);
     DeductBalanceResponse deductBalance(String accountNumber, BigDecimal amount);
    void creditBalance(String accountNumber, BigDecimal amount);

    void blockAccount(String accountNumber);
}