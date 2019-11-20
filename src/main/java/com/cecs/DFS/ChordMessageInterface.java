package com.cecs.DFS;

import java.rmi.*;

public interface ChordMessageInterface extends Remote {
    ChordMessageInterface getPredecessor() throws RemoteException;

    ChordMessageInterface locateSuccessor(long key) throws RemoteException;

    ChordMessageInterface closestPrecedingNode(long key) throws RemoteException;

    void joinRing(String Ip, int port) throws RemoteException;

    void joinRing(ChordMessageInterface successor) throws RemoteException;

    void notify(ChordMessageInterface j) throws RemoteException;

    boolean isAlive() throws RemoteException;

    long getId() throws RemoteException;

    void put(long guidObject, RemoteInputFileStream inputStream) throws RemoteException;

    void put(long guidObject, String text) throws RemoteException;

    RemoteInputFileStream get(long guidObject) throws RemoteException;

    byte[] get(long guidObject, long offset, int len) throws RemoteException;

    void delete(long guidObject) throws RemoteException;
}
