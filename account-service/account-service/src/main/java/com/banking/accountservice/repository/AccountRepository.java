package com.banking.accountservice.repository;

import com.banking.accountservice.entity.Account;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountRepository extends JpaRepository<Account,String> {
    boolean existsByEmail(@NotBlank(message = "Email is required") @Email(message = "Email should be valid") String email);

    boolean existsByAccountNumber(String accountNumber);

    Account findByAccountNumber(String accountNumber);
}
