package io.streamingledger.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String transactionId;
    private String accountId;
    private String customerId;
    private Long eventTime;
    private String eventTimeFormatted;
    private String type;
    private String operation;
    private Double amount;
    private Double balance;
}