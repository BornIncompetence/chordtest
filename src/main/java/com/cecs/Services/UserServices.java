package com.cecs.Services;

import static java.util.Arrays.binarySearch;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.cecs.DFS.DFS;
import com.cecs.DFS.RemoteInputFileStream;
import com.cecs.Models.User;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class UserServices {
    private Gson gson = new GsonBuilder().setPrettyPrinting().create();
    public DFS dfs;
    public UserServices(DFS dfs) {
        this.dfs = dfs;
    }

    /**
     * Loads the user playlist from server, will return null if the authentication
     * fails
     * 
     * @param user The user whose playlist is being updated
     * @return <code>true</code> if update is successful and <code>false</code>
     *         otherwise
     * @throws Exception
     */
    public boolean updateUser(User user) throws Exception {
        var users = loadUsers();

        if (users == null) {
            return false;
        } else {
            for (var u : users) {
                if (user.username.equalsIgnoreCase(u.username)) {
                    u.setUserPlaylists(user.userPlaylists);
                    break;
                }
            }
        }

        var jsonUsers = gson.toJson(users);
        var writer = new FileWriter("users.json");
        writer.write(jsonUsers);
        writer.close();
        dfs.delete("users");
        dfs.create("users");
        dfs.append("users", new RemoteInputFileStream("users.json"));
        return true;
    }

    public boolean deleteAccount(User user) throws Exception {
        var users = loadUsers();
        var newUsers = new User[users.length - 1];

        // Copy all Users to new array except for deleted User
        int deleteIdx = binarySearch(users, user);
        if (deleteIdx < 0) {
            return false;
        }
        System.arraycopy(users, 0, newUsers, 0, deleteIdx);
        System.arraycopy(users, deleteIdx + 1, newUsers, deleteIdx, newUsers.length - deleteIdx);

        // Create string from array of Users and write to file
        var jsonUsers = gson.toJson(newUsers);
        var writer = new FileWriter("users.json");
        writer.write(jsonUsers);
        writer.close();
        dfs.delete("users");
        dfs.create("users");
        dfs.append("users", new RemoteInputFileStream("users.json"));
        return true;
    }

    /**
     * Function to create a new User and add the User to a JSON file
     *
     * @param username Name of user
     * @param password Password of user
     *
     * @return <code>true</code> If new user is added to file, <code>false</code> if
     *         the username already exists
     * @throws Exception
     */
    public boolean createAccount(String username, String password) throws Exception {
        var newUser = new User(username, password);
        var users = loadUsers();

        User[] newUsers;
        if (users == null) {
            newUsers = new User[] { newUser };
        } else {
            var len = users.length;
            newUsers = new User[len + 1];

            // Check is username is already taken
            for (var user : users) {
                if (newUser.username.equalsIgnoreCase(user.username)) {
                    return false;
                }
            }

            // Append new User to old User list
            System.arraycopy(users, 0, newUsers, 0, len);
            newUsers[len] = newUser;
            Arrays.sort(newUsers);
        }

        // Create string from array of Users and write to file
        var jsonUsers = gson.toJson(newUsers);
        var writer = new FileWriter("users.json");
        writer.write(jsonUsers);
        writer.close();
        dfs.delete("users");
        dfs.create("users");
        dfs.append("users", new RemoteInputFileStream("users.json"));
        return true;
    }

    /**
     * Loads the user playlist from server, will return null if the authentication
     * fails
     * 
     * @param username Username of user
     * @param password Password of user
     * @return <code>ArrayList</code> of user from the server if the user is found,
     *         or <code>null</code> if the user is not found
     * @throws Exception
     */
    public User login(String username, String password) throws Exception {
        var users = loadUsers();

        return Arrays.stream(users).filter(it -> it.username.equalsIgnoreCase(username) && it.password.equals(password))
                .findFirst().orElse(null);
    }

    /**
     * Loads the users.json file into the program.
     * 
     * @throws Exception
     */
    private User[] loadUsers() throws Exception {
        RemoteInputFileStream rifs;
        rifs = dfs.read("users", 0);
        rifs.connect();
        InputStreamReader reader = new InputStreamReader(rifs);
        String json = new BufferedReader(reader).lines().collect(Collectors.joining("\n"));
        User[] users = gson.fromJson(json, User[].class);
        return users;
    }
}