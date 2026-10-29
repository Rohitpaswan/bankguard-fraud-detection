package com.banking.transactionservice.service;

import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;

import java.util.List;

public interface TransactionService {
    TransactionResponse transfer(TransferRequest request);
    TransactionResponse getTransaction(String transactionId);
    List<TransactionResponse> getTransactionHistory(String accountNumber);
    void completeTransaction(Transaction transaction);
     void processCleanResult(String transactionID);
}
