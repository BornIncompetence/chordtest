package com.cecs.Services;

import com.cecs.Models.Music;
import com.google.gson.GsonBuilder;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class MusicServices {
    private static Music[] library;
    private static HashMap<String, List<Music>> queries = new HashMap<>();

    public MusicServices() {
    }

    public static Music[] loadSongs(String asdf) {
        if (library == null) {
            loadLibrary();
        }
        return library;
    }

    public static int querySize(String query) {
        if (query.isBlank()) {
            return library.length;
        }
        return queries.get(query).size();
    }

    public static Music[] loadChunk(int start, int end, String query) {
        if (library == null) {
            loadLibrary();
        }
        if (query.isBlank()) {
            var toIndex = Integer.min(end, library.length);
            return Arrays.copyOfRange(library, start, toIndex);
        }
        if (queries.containsKey(query)) {
            var musics = queries.get(query);
            var toIndex = Integer.min(end, musics.size());
            return musics.subList(start, toIndex).toArray(new Music[0]);
        }
        var musics = Arrays.stream(library)
                .filter(music -> music.getArtist().toString().toLowerCase().contains(query)
                        || music.getRelease().toString().toLowerCase().contains(query)
                        || music.getSong().toString().toLowerCase().contains(query))
                .collect(Collectors.toList());
        queries.put(query, musics);
        var toIndex = Integer.min(end, musics.size());
        return musics.subList(start, toIndex).toArray(new Music[0]);

    }

    private static void loadLibrary() {
        var reader = new InputStreamReader(App.class.getResourceAsStream("/music.json"), StandardCharsets.UTF_8); //change to use DFS.read
        var musics = new GsonBuilder().create().fromJson(reader, Music[].class);
        for (var music : musics) {
            music.getSong().setArtist(music.getArtist().getName());
        }
        library = musics;
    }
}
