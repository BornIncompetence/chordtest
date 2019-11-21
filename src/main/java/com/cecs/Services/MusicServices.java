package com.cecs.Services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.cecs.DFS.DFS;
import com.cecs.DFS.DFS.FileJson;
import com.cecs.Models.Music;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MusicServices {
    private Music[] library;
    // private HashMap<String, List<Music>> queries = new HashMap<>();
    public DFS dfs;
    public Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public int library_size;

    public MusicServices(DFS dfs) {
        this.dfs = dfs;
        this.library_size = 0;
    }

    public Music[] loadChunk(int start, int end, String query) {
        System.out.println("run loadChunk");
        List<Music> list = new ArrayList<>();
        try {
            FileJson mf = dfs.searchFile("music");
            int numOfPages = mf.getNumOfPages();
            Thread[] threads = new Thread[numOfPages];
            Music[][] musics = new Music[numOfPages][];
            for (int i = 0; i < numOfPages; i++) {
                long guid = mf.getPage(i).getGuid();
                var peer = dfs.getChord().locateSuccessor(guid);
                int idx = i;
                threads[idx] = new Thread() {
                    public void run() {
                        try {
                            System.out.println("Enter thread");
                            String json = peer.search(guid, query);
                            musics[idx] = gson.fromJson(json, Music[].class);

                        } catch (Exception e) {
                            System.out.println("in Thread " + e);
                            // TODO: handle exception
                        }
                    }
                };
                // Runtime.getRuntime().addShutdownHook(threads[idx]);
                threads[idx].start();

            }
            System.out.println("test 1");
            for (Thread t : threads) {
                t.join();
            }
            System.out.println("test 2");
            for (Music[] musicList : musics) {
                System.out.println(musicList.length);
                list.addAll(Arrays.asList(musicList));
            }

            System.out.println("test 3");
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("in loadChunk " + e);
        }
        var toIndex = Integer.min(end, list.size());
        return list.subList(start, toIndex).toArray(new Music[0]);
    }
}
