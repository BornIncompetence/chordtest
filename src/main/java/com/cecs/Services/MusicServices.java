package com.cecs.Services;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.cecs.DFS.DFS;
import com.cecs.DFS.RemoteInputFileStream;
import com.cecs.Models.Music;
import com.google.gson.Gson;

import org.apache.commons.lang3.ArrayUtils;

public class MusicServices {
    private Music[] library;
    private HashMap<String, List<Music>> queries = new HashMap<>();
    public DFS dfs;
    public Gson gson = new Gson();

    public MusicServices(DFS dfs) {
        this.dfs = dfs;
    }

    public Music[] loadSongs(String asdf) throws Exception {
        if (library == null) {
            loadLibrary();
        }
        return library;
    }

    public int querySize(String query) {
        if (query.isBlank()) {
            return library.length;
        }
        return queries.get(query).size();
    }

    public Music[] loadChunk(int start, int end, String query) {
        if (library == null) {
            try {
                loadLibrary();
            } catch (Exception e) {
                e.printStackTrace();
            }
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

    private void loadLibrary() {
        RemoteInputFileStream rifs;
        try {
            rifs = dfs.read("music", 0);
            rifs.connect();
            InputStreamReader reader = new InputStreamReader(rifs);
            String json = new BufferedReader(reader).lines().collect(Collectors.joining("\n"));
            Music[] musics = gson.fromJson(json, Music[].class);
            for (var music : musics) {
                music.getSong().setArtist(music.getArtist().getName());
            }
            library = musics;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
