package com.cecs.Services;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Base64;

import com.cecs.DFS.RemoteInputFileStream;

public class SongServices {
    private static final int FRAGMENT_SIZE = 16384;

    public SongServices() {
    }

    /*
     * getSongChunk: Gets a chunk of a given song
     *
     * @param key: Song ID. Each song has a unique ID
     *
     * @param fragment: The chunk corresponds to [fragment * FRAGMENT_SIZE,
     * FRAGMENT_SIZE]
     */
    public static String getSongChunk(String id, Long fragment) throws IOException {
        byte[] buf = new byte[FRAGMENT_SIZE];

        var filename = String.format("%s.mp3", id);
        //var inputStream = new FileInputStream(filename);
        var inputStream = new RemoteInputFileStream(filename);
        inputStream.skip(fragment * FRAGMENT_SIZE);
        inputStream.read(buf);
        inputStream.close();
        // Encode in base64 so it can be transmitted
        return Base64.getEncoder().encodeToString(buf);
    }

    /*
     * getFileSize: Gets a size of the file
     *
     * @param key: Song ID. Each song has a unique ID
     */
    public static int getFileSize(String id) {
        var filename = String.format("%s.mp3", id);

        return (int) new File(filename).length();
    }
}
