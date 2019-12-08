package com.cecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import com.cecs.DFS.DFS;
import com.cecs.DFS.RemoteInputFileStream;
import com.cecs.Server.Communication;

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
            String[] args = line.split("\\s");
            if (args[0].equals("join") && args.length > 1) {
                if(p != Integer.parseInt(args[1])){
                    try {
                        dfs.join("127.0.0.1", Integer.parseInt(args[1]));
                    } catch (NumberFormatException e) {
                        System.err.println("Could not parse Integer from " + args[1]);
                    }
                }
            }
            if (args[0].equals("print")) {
                dfs.print();
            }
            if (args[0].equals("ls")) {
                System.out.println(dfs.listFiles());
            }
            if (args[0].equals("leave")) {
                dfs.leave();
            }
            if (args[0].equals("touch") && args.length > 1) {
                dfs.create(args[1]);
            }
            if (args[0].equals("delete") && args.length > 1) {
                dfs.delete(args[1]);
            }
            if (args[0].equals("read") && args.length > 2) {
                try {
                    dfs.read(args[1], Integer.parseInt(args[2]));
                } catch (NumberFormatException e) {
                    System.err.println("Could not parse Integer from " + args[2]);
                }
            }
            if (args[0].equals("head") && args.length > 1) {
                dfs.head(args[1]);
            }
            if (args[0].equals("tail") && args.length > 1) {
                dfs.tail(args[1]);
            }
            if (args[0].equals("append") && args.length > 2) {
                // Check if it is a directory
                /*
                var file = new File(args[2]);
                if (file.exists()) {
                    if (file.isDirectory()) {
                        // Add all files in directory
                        var dir = file.listFiles();
                        assert dir != null;

                        for (var f : dir) {
                            var appendant = new RemoteInputFileStream(f);
                            dfs.append(args[1], appendant);
                        }
                        System.out.println("Pages added");
                    } else {
                        // Add single file
                        var appendant = new RemoteInputFileStream(file);
                        dfs.append(args[1], appendant);
                        System.out.println("Page added");
                    }
                }
                */
                dfs.append(args[1], args[2]);
                System.out.println("page added");
            }
            if (args[0].equals("move") && args.length > 2) {
                dfs.move(args[1], args[2]);
            }
            if (args[0].equals("server")) {
                var port = 5500;
                try {
                    if (args.length > 1) {
                        port = Integer.parseInt(args[1]);
                    }
                    // Open server
                    var comm = new Communication(port, 32768, dfs);
                    comm.openConnection();
                } catch (IOException e) {
                    System.err.println("The server has encountered an error.");
                    e.printStackTrace();
                } catch (NumberFormatException e) {
                    System.err.println("Could not parse Integer from " + args[1]);
                }
            }
            if (args[0].equals("pull")){
                if(args.length > 1){
                    dfs.pull(args[1], Integer.parseInt(args[2]));
                }
            }
            if (args[0].equals("push")){
                dfs.push();
            }
            System.out.print("> ");
            System.out.flush();
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