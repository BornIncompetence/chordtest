package com.cecs.Models;

import java.util.ArrayList;

public class User implements Comparable<User> {
    public String username;
    public String password;
    public ArrayList<Playlist> userPlaylists;

    public User(String name, String pass) {
        this.username = name;
        this.password = pass;
        this.userPlaylists = new ArrayList<Playlist>();
    }

    public Boolean containsPlaylist(String playlistName) {
        for (Playlist playlist : userPlaylists) {
            if (playlist.getName().equals(playlistName))
                return true;
        }
        return false;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public ArrayList<Playlist> getUserPlaylists() {
        return userPlaylists;
    }

    public void setUserPlaylists(ArrayList<Playlist> userPlaylists) {
        this.userPlaylists = userPlaylists;
    }

    public void changeUsername(String newUsername) {
        this.username = newUsername;
    }

    public boolean changePassword(String oldPassword, String newPassword) {
        if (this.password.equals((oldPassword))) {
            this.password = newPassword;
            return true;
        } else {
            return false;
        }
    }

    public boolean addNewPlaylist(Playlist newPlaylist) {
        return this.userPlaylists.add(newPlaylist);
    }

    public boolean deletePlaylist(Playlist playlistToRemove) {
        return this.userPlaylists.remove((playlistToRemove));
    }

    public Playlist findPlaylistByName(String name) {
        for (Playlist playlist : userPlaylists) {
            if (playlist.getName().equals(name))
                return playlist;
        }
        return null;
    }

    // I suggest naming this logout
    public void deleteUser() {
        this.username = null;
        this.password = null;
        this.userPlaylists = null;
    }

    public boolean IsValidPassword(String pass) {
        if (this.password.equals((pass))) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(User other) {
        var userCmp = this.username.compareToIgnoreCase(other.username);
        var passCmp = this.password.compareTo(other.password);
        if (userCmp == 0) {
            return passCmp;
        }
        return userCmp;
    }
}
