package com.cecs.Services;

import com.cecs.DFS;
import com.cecs.RemoteInputFileStream;
import com.cecs.Models.Music;
import com.google.gson.GsonBuilder;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class MusicServices {
    private Music[] library;
    private HashMap<String, List<Music>> queries = new HashMap<>();
    private DFS dfs;

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

    public Music[] loadChunk(int start, int end, String query) throws Exception {
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

    private void loadLibrary() throws Exception {
        RemoteInputFileStream is = dfs.read("music.json", 1);
        is.connect();
        var reader = new InputStreamReader(is);
        var musics = new GsonBuilder().create().fromJson(reader, Music[].class);
        for (var music : musics) {
            music.getSong().setArtist(music.getArtist().getName());
        }
        library = musics;
    }
}
