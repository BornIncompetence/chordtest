package com.cecs.DFS;

import com.cecs.Models.Transaction;

public interface AtomicCommitInterface {
    Boolean canCommit(Transaction trans);
    void commit(Transaction trans);
    void abort(Transaction trans);
    Boolean hasBeenCommitted(Transaction trans);
    Boolean getDecision(Transaction trans);
}