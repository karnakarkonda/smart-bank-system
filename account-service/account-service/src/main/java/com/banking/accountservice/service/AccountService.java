package com.banking.accountservice.service;


import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;

    private final SecureRandom secureRandom;

    public AccountResponse createAccount(CreateAccountRequest createAccountRequest) {
        log.info("Creating account for: {}", createAccountRequest.getEmail());
        boolean isAccountAlreadyExists = accountRepository.existsByEmail(createAccountRequest.getEmail());
        if (isAccountAlreadyExists) {
            log.warn("Account already exists for email: {}", createAccountRequest.getEmail());
            throw new IllegalArgumentException("Account already exists");
        }
        Account account = Account.builder()
                .accountHolderName(createAccountRequest.getAccountHolderName())
                .accountType(createAccountRequest.getAccountType())
                .email(createAccountRequest.getEmail())
                .phone(createAccountRequest.getPhone())
                .status(AccountStatus.ACTIVE)
                .balance(createAccountRequest.getInitialBalance())
                .accountNumber(generateAccountNumber())
                .dailyTransactionLimit(
                        createAccountRequest.getAccountType() == AccountType.SAVINGS
                                ? new BigDecimal(100000)
                                : new BigDecimal(500000)
                )
                .build();

        Account savedAccount = accountRepository.save(account);
        log.info("Account created for: {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

    /**
     * get account by account number
     * @param accountNumber
     * */
    public AccountResponse getAccount(String accountNumber) {
        log.info("Getting account for: {}", accountNumber);
        Account fetchedAccount = accountRepository.findByAccountNumber(accountNumber);
        if (fetchedAccount == null) {
            log.warn("Account not found for account number: {}", accountNumber);
            throw new IllegalArgumentException("Account not found");
        }
        log.info("Account found for: {}", fetchedAccount.getAccountNumber());
        return mapToResponse(fetchedAccount);
    }
    /**
     * getting balance by account number
     * @param accountNumber
     * */
    public BigDecimal getBalance(String accountNumber) {
        log.info("Getting balance for: {}", accountNumber);
        Account fetchedAccount = accountRepository.findByAccountNumber(accountNumber);
        if (fetchedAccount == null) {
            log.warn("Account not found for account number: {}", accountNumber);
            throw new IllegalArgumentException("Account not found");
        }
        log.info("Balance found for: {}", fetchedAccount.getAccountNumber());
        return fetchedAccount.getBalance();
    }
/**
* Block account - called by Fraud detection service via kafka
 * @param accountNumber
* */
    public String blockAccount(String accountNumber) {
        log.info("Blocking account for: {}", accountNumber);
        Account fetchedAccount = accountRepository.findByAccountNumber(accountNumber);
        if (fetchedAccount == null) {
            log.warn("Account not found for account number: {}", accountNumber);
            throw new IllegalArgumentException("Account not found");
        }
        fetchedAccount.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(fetchedAccount);
        log.info("Account blocked for: {}", accountNumber);
        return "Account blocked";
    }

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting balance for: {}", accountNumber);
        Account fetchedAccount = accountRepository.findByAccountNumber(accountNumber);
        if (fetchedAccount == null) {
            log.warn("Account not found for account number: {}", accountNumber);
            throw new IllegalArgumentException("Account not found");
        }

        if(fetchedAccount.getStatus()!=AccountStatus.ACTIVE){
            throw  new IllegalArgumentException("Account not active");
        }

        if(fetchedAccount.getBalance().compareTo(amount)<0){
            throw  new IllegalArgumentException("Insufficient balance");
        }
        fetchedAccount.setBalance(fetchedAccount.getBalance().subtract(amount));
        accountRepository.save(fetchedAccount);
        log.info("Balance deducted new balance is : {}", fetchedAccount.getBalance());

    }

   public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Credit balance for: {}", accountNumber);
        Account fetchedAccount = accountRepository.findByAccountNumber(accountNumber);
        if (fetchedAccount == null) {
            log.warn("Account not found for account number: {}", accountNumber);
            throw new IllegalArgumentException("Account not found");
        }
        if(fetchedAccount.getStatus()!=AccountStatus.ACTIVE){
            throw  new IllegalArgumentException("Account not active");
        }

        fetchedAccount.setBalance(fetchedAccount.getBalance().add(amount));
        accountRepository.save(fetchedAccount);
        log.info("Balance credited for: {}", fetchedAccount.getBalance());

   }

    private AccountResponse mapToResponse(Account savedAccount) {
        return AccountResponse.builder()
                .id(savedAccount.getId())
                .accountNumber(savedAccount.getAccountNumber())
                .accountHolderName(savedAccount.getAccountHolderName())
                .accountType(savedAccount.getAccountType())
                .email(savedAccount.getEmail())
                .phone(savedAccount.getPhone())
                .status(savedAccount.getStatus())
                .balance(savedAccount.getBalance())
                .dailyTransactionLimit(savedAccount.getDailyTransactionLimit())
                .createdAt(savedAccount.getCreatedAt())
                .build();
    }

    // Generate unique 12 digit account number
    private String generateAccountNumber() {
        String accountNumber;
        do {
            long randomNumber = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", randomNumber);
        }
        while (accountRepository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }
}
