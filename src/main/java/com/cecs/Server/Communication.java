package com.cecs.Server;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.NoSuchElementException;

import com.cecs.DFS.DFS;
import com.cecs.Models.User;
import com.cecs.Services.MusicServices;
import com.cecs.Services.SongServices;
import com.cecs.Services.UserServices;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Communication {
    private final int port;
    private final byte[] buffer;
    private final HashMap<String, Object> listOfObjects = new HashMap<>();
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public Communication(int port, int size, DFS dfs) {
        this.port = port;
        this.buffer = new byte[size];
        this.listOfObjects.put("SongServices", new SongServices(dfs));
        this.listOfObjects.put("UserServices", new UserServices(dfs));
        this.listOfObjects.put("MusicServices", new MusicServices(dfs));
    }

    public void openConnection() throws IOException {
        var socket = new DatagramSocket(port);
        while (true) {
            // Receive
            final var inbound = new DatagramPacket(buffer, buffer.length);
            System.out.println("Listening...");
            socket.receive(inbound);

            // Break out of loop if server receives end, else do an action
            final var message = new String(inbound.getData(), 0, inbound.getLength());
            if (message.equals("end")) {
                System.out.println("Server is closing...");
                break;
            }
            final var out = dispatch(message).getBytes();

            // Get client info
            final var address = inbound.getAddress();
            final var port = inbound.getPort();

            // Send back to client
            final var outboundPacket = new DatagramPacket(out, out.length, address, port);
            System.out.format("Sending message of size %s to %s\n", out.length, address);
            socket.send(outboundPacket);
        }
        socket.close();
    }

    /**
     * Parses and invokes the method as described in the passed in JSON string
     * Returns the result of invoking the method if the request string is valid.
     *
     * @param request The JSON object represented as a string
     *
     * @return String representing either the return value of the function, or an
     *         error if one occurred
     */
    private String dispatch(String request) {
        var jsonReturn = new JsonObject();
        var parser = new JsonParser();
        var jsonRequest = parser.parse(request).getAsJsonObject();

        try {
            // Obtains the object pointing to SongServices
            var object = listOfObjects.get(jsonRequest.get("objectName").getAsString());

            // Obtains the method from the list of methods that exist for the class
            var optionalMethod = Arrays.stream(object.getClass().getMethods())
                    .filter(it -> it.getName().equals(jsonRequest.get("remoteMethod").getAsString())).findFirst();
            if (optionalMethod.isEmpty()) {
                jsonReturn.addProperty("error", "Method does not exist");
                return jsonReturn.toString();
            }
            var method = optionalMethod.get();

            // Prepare the parameters
            var types = method.getParameterTypes();
            var arr = jsonRequest.get("param").getAsJsonArray();
            if (types.length != arr.size()) {
                throw new NoSuchElementException();
            }

            var parameters = new Object[types.length];
            for (int i = 0; i < types.length; ++i) {
                switch (types[i].getCanonicalName()) {
                case "java.lang.Long":
                case "long":
                    parameters[i] = arr.get(i).getAsLong();
                    break;
                case "java.lang.Integer":
                case "int":
                    parameters[i] = arr.get(i).getAsInt();
                    break;
                case "java.lang.String":
                    parameters[i] = arr.get(i).getAsString();
                    break;
                case "com.cecs.model.User":
                    parameters[i] = gson.fromJson(arr.get(i), User.class);
                    break;
                }
            }

            var ret = method.invoke(object, parameters);
            jsonReturn.addProperty("ret", gson.toJson(ret));

        } catch (InvocationTargetException | IllegalAccessException e) {
            var errorField = String.format("Error on %s.%s()", jsonRequest.get("objectName").getAsString(),
                    jsonRequest.get("remoteMethod").getAsString());
            jsonReturn.addProperty("error", errorField);
        } catch (NoSuchElementException e) {
            var errorField = String.format("Wrong number of parameters provided for %s.%s",
                    jsonRequest.get("objectName").getAsString(), jsonRequest.get("remoteMethod").getAsString());
            jsonReturn.addProperty("error", errorField);
        }

        return jsonReturn.toString();
    }
}