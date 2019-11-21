package com.cecs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.cecs.DFS.DFS;
import com.cecs.DFS.RemoteInputFileStream;
import com.cecs.Models.Music;
import com.cecs.Server.Communication;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

/**
 * Hello world!
 */
public final class DFSCommand {
    DFS dfs;

    public DFSCommand(int p, int portToJoin) throws IOException {
        dfs = new DFS(p);

        if (portToJoin > 0) {
            System.out.println("Joining " + portToJoin);
            dfs.join("127.0.0.1", portToJoin);
        }

        BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
        for (String line = ""; !line.equals("quit"); line = buffer.readLine()) {
            String[] result = line.split("\\s");
            if (result[0].equals("join") && result.length > 1) {
                dfs.join("127.0.0.1", Integer.parseInt(result[1]));
            }
            if (result[0].equals("print")) {
                dfs.print();
            }
            if (result[0].equals("ls")) {
                System.out.println(dfs.lists());
            }
            if (result[0].equals("leave")) {
                dfs.leave();
            }
            if (result[0].equals("touch")) {
                dfs.create(result[1]);
            }
            if (result[0].equals("delete")) {
                dfs.delete(result[1]);
            }
            if (result[0].equals("read")) {
                dfs.read(result[1], Integer.parseInt(result[2]));
            }
            if (result[0].equals("head")) {
                dfs.head(result[1]);
            }
            if (result[0].equals("tail")) {
                dfs.tail(result[1]);
            }
            if (result[0].equals("append")) {
                RemoteInputFileStream fileToAppend = new RemoteInputFileStream(result[2]);
                dfs.append(result[1], fileToAppend);
                System.out.println("Page added");
            }
            if (result[0].equals("move")) {
                dfs.move(result[1], result[2]);
            }
            if (result[0].equals("server")) {
                var comm = new Communication(5500, 32768, dfs);
                try {
                    comm.openConnection();
                } catch (IOException e) {
                    System.err.println("The server has encountered an error.");
                    e.printStackTrace();
                }
            }
        }
        // User interface:
        // join, ls, touch, delete, read, tail, head, append, move
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     * @throws IOException              If IO error occurs within the command loop
     * @throws IllegalArgumentException If the incorrect parameters are provided
     * @throws NumberFormatException    If number arguments cannot be parsed
     */
    public static void main(String[] args) throws IOException, IllegalArgumentException, NumberFormatException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Parameter: <port> <portToJoin>");
        }
        if (args.length > 1) {
            new DFSCommand(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        } else {
            new DFSCommand(Integer.parseInt(args[0]), 0);
        }
    }
}