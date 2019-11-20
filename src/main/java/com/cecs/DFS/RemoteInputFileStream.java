package com.cecs.DFS;

import static java.net.InetAddress.getLoopbackAddress;

/*
 * RemoteInputFileStream Implements an Input Stream for big
 * files. It creates a server and return the address
 * The client must call connect() before reading
 *
 * @author  Oscar Morales-Ponce
 * @version 0.16
 * @since   03-3-2019
 */

import java.io.*;
import java.net.*;
import java.util.concurrent.Semaphore;

public class RemoteInputFileStream extends InputStream implements Serializable {
    private static final long serialVersionUID = 7006430612917340346L;
    public InetAddress IP;
    public int port;
    public int total;
    public int pos;
    public InputStream input;
    public Semaphore sem;
    private static int BUFFER_LENGTH = 1 << 16;
    /**
     * It stores a buffer with FRAGMENT_SIZE bytes for the current reading. This
     * variable is useful for UDP sockets. Thus bur is the datagram
     */
    protected byte[] buf;
    /**
     * It prepares for the nuext buffer. In UDP sockets you can read nextbufer while
     * buf is in use
     */
    protected byte[] nextBuf;
    /**
     * It is used to read the buffer
     */
    protected int fragment = 0;

    /**
     * Connects to the server to provide the file
     */
    public void connect() {
        this.nextBuf = new byte[BUFFER_LENGTH];
        pos = 0;
        try {
            Socket socket = new Socket(getLoopbackAddress(), port);
            input = socket.getInputStream();
            sem = new Semaphore(1);
            sem.acquire();
            getBuff(fragment);
            fragment++;
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts a server to provide the file
     */
    public RemoteInputFileStream(String pathName, boolean deleteAfter) throws IOException {
        File file = new File(pathName);
        total = (int) file.length();
        pos = 0;

        ServerSocket serverSocket = new ServerSocket(0);
        port = serverSocket.getLocalPort();
        IP = serverSocket.getInetAddress();

        new Thread(() -> {
            try {
                Socket socket = serverSocket.accept();
                OutputStream socketOutputStream = socket.getOutputStream();
                FileInputStream is = new FileInputStream(pathName);
                byte[] b = new byte[BUFFER_LENGTH];
                while (is.available() > 0) {
                    is.read(b);
                    socketOutputStream.write(b);
                }
                is.close();
                if (deleteAfter) {
                    file.delete();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public RemoteInputFileStream(String pathName) throws IOException {
        this(pathName, false);
    }

    /**
     * getNextBuff reads the buffer. It gets the data using the remote method
     * getSongChunk
     */
    protected void getBuff(int fragment) {
        new Thread(() -> {
            try {
                while ((total / BUFFER_LENGTH <= fragment || input.available() < BUFFER_LENGTH)
                        && (total / BUFFER_LENGTH > fragment || (input.available() < total % BUFFER_LENGTH))) {

                    Thread.sleep(1);
                }
                input.read(nextBuf);
                sem.release();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Reads the next byte of data from the input stream.
     */
    @Override
    public synchronized int read() {

        if (pos >= total) {
            pos = 0;
            return -1;
        }
        int posmod = pos % BUFFER_LENGTH;
        if (posmod == 0) {
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            buf = nextBuf.clone();

            getBuff(fragment);
            fragment++;
        }
        int p = pos % BUFFER_LENGTH;
        pos++;
        return buf[p] & 0xff;
    }

    /**
     * Reads some number of bytes from the input stream and stores them into the
     * buffer array b.
     */
    @Override
    public synchronized int read(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (pos >= total) {
            return -1;
        }
        int avail = total - pos;
        if (len > avail) {
            len = avail;
        }
        if (len <= 0) {
            return 0;
        }
        for (int i = off; i < off + len; i++)
            b[i] = (byte) read();
        return len;
    }

    public int available() {
        return total - pos;
    }

}
