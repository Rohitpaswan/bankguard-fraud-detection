package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.DeductBalanceResponse;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.reposititory.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
@Service
public class TransactionServiceImpl implements TransactionService{

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final ModelMapper mapper;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";
    private static final String VERIFICATION_OTP = "verification:otp:";
    private static final long MAX_OTP_ATTEMPTS = 3;


    public TransactionResponse transfer(TransferRequest request){

        log.info("SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // SAGA STEP 1: Deduct from sender
        DeductBalanceResponse deductBalaceResponse =  accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount());



        Transaction transaction = Transaction.builder()
                .senderAccountNumber(request.getSenderAccountNumber())
                .receiverAccountNumber(request.getReceiverAccountNumber())
                .amount(request.getAmount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(request.getDescription())
                .referenceNumber(UUID.randomUUID().toString())
                .build();

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        // SAGA STEP - 2: Publish for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription(),
                deductBalaceResponse.getBalanceBefore()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent published: {}", savedTransaction.getId());


        return  mapper.map(savedTransaction, TransactionResponse.class);

    }

    public TransactionResponse getTransaction(String transactionId) {

        Transaction transaction = transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found: " + transactionId));

        return mapper.map(transaction, TransactionResponse.class);
    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber){

        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(transaction -> mapper.map(transaction, TransactionResponse.class))
                .collect(Collectors.toList());
    }

    @Override
    public void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

        log.info("SAGA COMPLETE - Transaction {} completed",
                transaction.getId());
    }

    public TransactionResponse verifyOTP(String transactionID, String otp){
        log.info("OTP verification for the transaction: {}", transactionID);


        Transaction transaction = transactionRepository.findById(transactionID)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found "+transactionID
                ));

        String key = getkey(transactionID);
        Object raw = redisTemplate.opsForHash().get(key, "otp");
        String storedOtp = raw == null ? null : raw.toString();


        if (transaction.getStatus() != TransactionStatus.PENDING_VERIFICATION) {
            throw new IllegalStateException("Transaction is not awaiting verification");
        }

        if(storedOtp == null){
            // OTP EXPIRED
            log.warn("OTP expired for transaction: {}", transactionID);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapper.map(transaction, TransactionResponse.class);
        }

        //check storedOtp with Otp get from user
        if(otp == null || !MessageDigest.isEqual(
                storedOtp.getBytes(StandardCharsets.UTF_8),
                otp.getBytes(StandardCharsets.UTF_8)
        )){
            Long attempts = redisTemplate.opsForHash().increment(key, "attempts", 1);
            if (attempts > MAX_OTP_ATTEMPTS) {
                redisTemplate.delete(key);
                blockAccountAndCompensate(transaction, "Wrong OTP entered 3 times - account blocked");
                return mapper.map(transaction, TransactionResponse.class);
            }

            log.info("Invalid OTP. Attempts remaining:{} ", MAX_OTP_ATTEMPTS - attempts);
            throw new IllegalArgumentException("Invalid OTP. Attempts remaining: " + (MAX_OTP_ATTEMPTS - attempts));


        }

        // OTP correct - complete transaction
        log.info("OTP verified - completing transaction: {}", transactionID);
        redisTemplate.delete(key);
        completeTransaction(transaction);
        return mapper.map(transaction, TransactionResponse.class);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        // CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                " - SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());

        transactionRepository.save(transaction);

        // PUBLISH refund event - Notification service will alert user
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

        log.info("SAGA COMPENSATION COMPLETE - {} refunded to  {}",
                transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason){

        // Publish fraud.detected -> Account Service will block account
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked, Kindly contact to the bank",
                transaction.getSenderAccountNumber());

        // SAGA COMPENSATION - refund Sender
        compensateTransaction(transaction, reason);
    }




    public void processCleanResult(String transactionID){

        Transaction transaction = transactionRepository.findById(transactionID)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found "+transactionID
                ));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING - skipping", transactionID);
            return;
        }

        completeTransaction(transaction);
    }

    //======================================
    // Helper Method
    //======================================

    private String getkey(String transactionID){
        return   VERIFICATION_OTP + transactionID ;
    }


    public Long getOtpTTL(String transactionId) {

        String key = "verification:otp:" + transactionId;

        return redisTemplate.getExpire(key, TimeUnit.SECONDS);


    }

}
