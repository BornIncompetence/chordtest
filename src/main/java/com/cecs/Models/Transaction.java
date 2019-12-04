package com.cecs.Models;

import java.time.LocalDateTime;
import java.util.UUID;

public class Transaction {
    Long TransactionId;
    public String fileName;
    public LocalDateTime ts;
    public int pageIndex;
    public String operation;

    public Transaction(String filename, int pageIndex, String Operation){
        this.TransactionId = UUID.randomUUID().getLeastSignificantBits();
        this.fileName = filename;
        this.ts = LocalDateTime.now();
        this.operation = Operation;
    }
}