package com.cecs.Models;

import java.time.LocalDateTime;
import java.util.UUID;

public class Transaction {
    public enum Operation { WRITE, DELETE};
    public enum Vote { YES, NO};
    Long TransactionId;
    Vote vote;
    Operation operation;
    String fileName;
    Long pageIndex;
    LocalDateTime ts;

    public Transaction(Vote vote, Operation operation, String filename, Long pageIndex){
        this.TransactionId = UUID.randomUUID().getLeastSignificantBits();
        this.vote = vote;
        this.operation = operation;
        this.fileName = filename;
        this.pageIndex = pageIndex;
        this.ts = LocalDateTime.now();
    }
}