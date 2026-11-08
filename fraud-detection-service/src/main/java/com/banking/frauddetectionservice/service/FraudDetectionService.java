package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Slf4j
@Service
public class FraudDetectionService {


    private final RedisTemplate<String, String> redisTemplate;


    private final KafkaTemplate<String, Object> kafkaTemplate;
    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    @Value("${fraud.detection.minimum-history}")
    private long miniHistory;

    @Value("${fraud.detection.new-account-limit}")
    private BigDecimal newAccountLimit;

    @Value("${fraud.detection.minAmountFloor}")
    private BigDecimal minAmountFloor;

    @Value("${fraud.detection.maxAmountMultiplier}")
    private BigDecimal maxAmountMultiplier;

    @Value("${fraud.detection.minHistory}")
    private BigDecimal minHistory;






    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";
    private static final String PROFILE_KEY_PREFIX = "fraud:profile";
    private static final long PROFILE_TTL_DAYS = 90;

    public void checkTransaction(Map<String,Object> payload){
        String transactionId = (String)payload.get("transactionId");
        String accountNumber = (String)payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        // sender balance before transaction
        BigDecimal senderBalance = new BigDecimal(payload.get("balanceBefore").toString());
        FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBalance);

        if(result.isFraud()){

            log.info("Suspicious activity detected - account: {} "+
                            "reason: {} - requesting OTP verification",
                    accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
        }

        else{
            // Transaction is clean
            log.info("Transaction clean");

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);
        }
    }




    private FraudCheckResult performFraudChecks(
            String accountNumber,
            BigDecimal amount,
            BigDecimal senderBalance) {

        // Pattern 1: Velocity Check
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(
                    true, "Too many transactions in 60 seconds "+
                    " - Velocity limit exceeded");
        }

        // Pattern 2: Amount check
        if(isAmountSuspicious(accountNumber, amount)){
            return new FraudCheckResult(
                    true, "Unusual transaction amount "+
                    " - exceeds 3x your average");
        }

        // Pattern 3: Balance Check
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBalance, amount)){
            return new FraudCheckResult(
                    true, "Transaction exceed 90% of account balance");
        }

        return new FraudCheckResult(false, null);
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal maxAllowed = senderBalance.multiply(
                BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {} maxAllowed: {} suscpious: {}",
                amount, maxAllowed, amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }


    private boolean isVelocityExceeded(String accountNumber){
        String key = "fraud:velocity" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1){
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account: {} count: {}/{}",
                accountNumber, count, maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        // 1. Read the account profile (count, sum, max) from Redis in one call
        Map<Object, Object> profile = redisTemplate.opsForHash().entries(profileKey(accountNumber));

        long count = (profile.get("count") == null) ? 0 : Long.parseLong(profile.get("count").toString());

        // 2. Cold start: not enough history, so use a fixed limit for new accounts

        if (count < miniHistory) {
            boolean suspicious = amount.compareTo(newAccountLimit) > 0;
            log.info("Cold-start check - account: {} amount: {} count: {} newAccountLimit: {} suspicious: {}",
                    accountNumber, amount, count, newAccountLimit, suspicious);
            return suspicious;
        }

        // 3. Safety: if the profile is incomplete, treat it as a new account
        if (profile.get("sum") == null || profile.get("max") == null) {
            boolean suspicious = amount.compareTo(newAccountLimit) > 0;
            log.warn("Incomplete profile for account: {} - falling back to newAccountLimit. suspicious: {}",
                    accountNumber, suspicious);
            return suspicious;
        }

        BigDecimal sum = new BigDecimal(profile.get("sum").toString());
        BigDecimal maxSingleAmount = new BigDecimal(profile.get("max").toString());

        // 4. Average transaction amount for this account
        BigDecimal avg = sum.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);

        // 5. Average threshold: avg x multiplier, but never lower than the minimum floor
        BigDecimal avgThreshold = avg
                .multiply(BigDecimal.valueOf(suspiciousAmountMultiplier))
                .max(minAmountFloor);

        // 6. Max threshold: the user's biggest past transaction x multiplier
        BigDecimal maxThreshold = maxSingleAmount.multiply(maxAmountMultiplier);

        // 7. Suspicious only if the amount exceeds BOTH thresholds
        boolean suspicious = amount.compareTo(avgThreshold) > 0
                && amount.compareTo(maxThreshold) > 0;

        log.info("Amount check - account: {} amount: {} count: {} avg: {} max: {} avgThreshold: {} maxThreshold: {} suspicious: {}",
                accountNumber, amount, count, avg, maxSingleAmount, avgThreshold, maxThreshold, suspicious);

        return suspicious;
    }



    //===============================================
    // Helpers
    //===============================================


    private String profileKey(String accountNumber){
        return  PROFILE_KEY_PREFIX + accountNumber;
    }


    private static final RedisScript<Long> UPDATE_MAX_SCRIPT = new DefaultRedisScript<>(
            """
            local current = redis.call('HGET', KEYS[1], 'max')
            local incoming = tonumber(ARGV[1])
            if (not current) or (incoming > tonumber(current)) then
                redis.call('HSET', KEYS[1], 'max', ARGV[1])
                return 1
            end
            return 0
            """,
            Long.class
    );

    public void recordCompleteTransaction(String transactionId, String senderAccountNumber, BigDecimal amount) {

        //idempotency check
        Boolean firstTime = redisTemplate.opsForValue()
                .setIfAbsent(
                        "fraud: processed:" + transactionId,
                       "1",
                       Duration.ofDays(2)
                );

        if( !Boolean.TRUE.equals(firstTime)){
            log.info("Duplicated complete event occur ignore- txt{}", transactionId);
            return;
        }

        String profile = profileKey(senderAccountNumber);
        redisTemplate.opsForHash().increment(profile,"count", 1);
        redisTemplate.opsForHash().increment(profile, "sum", amount.doubleValue());
        Long isNewMax = redisTemplate.execute(UPDATE_MAX_SCRIPT, List.of(profile), amount.toPlainString());
        if (Long.valueOf(1L).equals(isNewMax)) {
            log.debug("New max amount for account {}: {}", senderAccountNumber, amount);
        }


    }
}
