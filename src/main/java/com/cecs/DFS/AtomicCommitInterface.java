package com.cecs.DFS;

import java.io.IOException;
import java.rmi.RemoteException;

import com.cecs.Models.Transaction;

public interface AtomicCommitInterface {
    Boolean canCommit(Transaction trans)throws RemoteException;
    void commit(Transaction trans) throws IOException;
}