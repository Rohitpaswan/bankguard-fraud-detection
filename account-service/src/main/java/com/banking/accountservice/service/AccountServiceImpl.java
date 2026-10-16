package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.security.SecureRandom;

import static org.hibernate.annotations.UuidGenerator.Style.RANDOM;

@Service
@RequiredArgsConstructor
@Slf4j
public class AccountServiceImpl implements AccountService{
    private final AccountRepository repository;
    private final ModelMapper modelMapper;
    private static final SecureRandom RANDOM = new SecureRandom();

    @Override
    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account for: {}", request.getEmail());

        if (repository.existsByEmail(request.getEmail())) {
            throw new IllegalArgumentException("Account already exists for email: " + request.getEmail());
        }
        // DTO -> Entity
        Account account = modelMapper.map(request, Account.class);

        // Fields that are not in the request
        account.setAccountNumber(generateAccountNumber());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit() != null ? request.getInitialDeposit(): BigDecimal.ZERO);
        Account saved = repository.save(account);

        //Entity - DTO
        return modelMapper.map(saved, AccountResponse.class);
    }

    private String generateAccountNumber() {
        String accountNumber;
        do {
            accountNumber = String.valueOf(
                    RANDOM.nextLong(100_000_000_000L, 1_000_000_000_000L));
        } while (repository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }
    @Override
    public AccountResponse getAccount(String accountNumber) {
        Account account = repository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("User Not Found"));
        return modelMapper.map(account, AccountResponse.class);

    }

    @Override
    public BigDecimal getBalance(String accountNumber) {
        Account account = repository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("User Not Found"));
        return account.getBalance();
    }

    @Transactional
    @Override
    public void blockAccount(String accountNumber) {
        log.info("Blocking account: {}", accountNumber);
        Account account = repository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("User Not Found"));
        account.setStatus(AccountStatus.BLOCKED);
        repository.save(account);
        log.info("Account blocked: {}", accountNumber);
    }


    @Transactional
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting {} from account: {}", amount, accountNumber);
        Account account = repository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("User Not Found"));

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active: " + accountNumber);
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient balance");
        }

        account.setBalance(account.getBalance().subtract(amount));
        repository.save(account);
        log.info("Balance deducted. New balance: {}", account.getBalance());
    }


    @Transactional
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting {} to account: {}", amount, accountNumber);
        Account account = repository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("User Not Found"));
        account.setBalance(account.getBalance().add(amount));
        repository.save(account);
        log.info("Balance credited. New balance: {}", account.getBalance());
    }

}
