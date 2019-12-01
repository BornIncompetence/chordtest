package com.cecs.Services;

import java.rmi.RemoteException;
import java.util.Base64;

import com.cecs.DFS.DFS;

public class SongServices {
    private static final int FRAGMENT_SIZE = 16384;
    DFS dfs;

    public SongServices(DFS dfs) {
        this.dfs = dfs;
    }

    /**
     * getSongChunk: Gets a chunk of a given song
     *
     * @param id: Song ID. Each song has a unique ID
     *
     * @param fragment: The chunk corresponds to [fragment * FRAGMENT_SIZE,
     * FRAGMENT_SIZE]
     */
    public String getSongChunk(String id, long fragment) throws RemoteException {
        byte[] buf;

        buf = dfs.getSong(id, fragment * FRAGMENT_SIZE, FRAGMENT_SIZE);
        // Encode in base64 so it can be transmitted
        return Base64.getEncoder().encodeToString(buf);
    }

    /**
     * getFileSize: Gets a size of the file
     *
     * @param id: Song ID. Each song has a unique ID
     */
    public int getFileSize(String id) throws RemoteException {
        return (int) dfs.getSongSize(id);
    }
}
