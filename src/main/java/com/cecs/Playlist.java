package com.cecs;

import java.util.ArrayList;

public class Playlist {
    private String name;
    private ArrayList<Song> songs;

    public Playlist() {
        this.name = "";
        this.songs = new ArrayList<>();
    }

    public Playlist(String name, ArrayList<Song> songs) {
        this.name = name;
        this.songs = songs;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<Song> getSongs() {
        return songs;
    }

    public void setSongs(ArrayList<Song> songs) {
        this.songs = songs;
    }

    public boolean addSong(Song songToAdd) {
        return songs.add(songToAdd);
    }

    public boolean removeSong(Song songToRemove) {
        return songs.remove(songToRemove);
    }

    public void clearPlaylist() {
        while (!songs.isEmpty()) {
            songs.remove(0);
        }
    }

}
