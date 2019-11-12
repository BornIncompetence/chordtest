package com.cecs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

/**
 * Hello world!
 */
public final class DFSCommand {
    DFS dfs;

    public DFSCommand(int p, int portToJoin) throws Exception {
        dfs = new DFS(p);

        if (portToJoin > 0) {
            System.out.println("Joining " + portToJoin);
            dfs.join("127.0.0.1", portToJoin);
        }

        BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
        String line = buffer.readLine();
        while (!line.equals("quit")) {
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
                //dfs.leave();
            }
            if (result[0].equals("tail")) {
                //dfs.leave();
            }
            if (result[0].equals("append")) {
                RemoteInputFileStream fileToAppend = new RemoteInputFileStream(result[2]);
                dfs.append(result[1], fileToAppend);
                System.out.println("Page added");
            }
            if (result[0].equals("move")) {
                dfs.move(result[1], result[2]);
            }
            line = buffer.readLine();
        }
        // User interface:
        // join, ls, touch, delete, read, tail, head, append, move
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     * @throws Exception
     * @throws NumberFormatException
     */
    public static void main(String[] args) throws NumberFormatException, Exception {
        Gson gson = new Gson();
        RemoteInputFileStream in = new RemoteInputFileStream("music.json", false);
        in.connect();
        Reader targetReader = new InputStreamReader(in);
        JsonReader jreader = new JsonReader(targetReader);
        Music[] music = gson.fromJson(jreader, Music[].class);
        System.out.println("Reading music successful!");

        if (args.length < 1) {
            throw new IllegalArgumentException("Parameter: <port> <portToJoin>");
        }
        if (args.length > 1) {
            DFSCommand dfsCommand = new DFSCommand(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        } else {
            DFSCommand dfsCommand = new DFSCommand(Integer.parseInt(args[0]), 0);
        }
    }
}
