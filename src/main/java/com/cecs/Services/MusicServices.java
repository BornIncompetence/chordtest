package com.cecs.Services;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.Semaphore;

import com.cecs.DFS.ChordMessageInterface;
import com.cecs.DFS.DFS;
import com.cecs.DFS.DFS.FileJson;
import com.cecs.Models.Music;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MusicServices {
    public DFS dfs;
    public Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private Semaphore sem;
    public int listSize; // store current size of list

    public MusicServices(DFS dfs) {
        this.dfs = dfs;
        this.listSize = 0;
        this.sem = new Semaphore(1, true);
    }

    public int getListSize() {
        return listSize;
    }

    /* Load some part of the music.json */
    public Music[] loadChunk(int start, int end, String query) {
        System.out.println("run loadChunk");
        List<Music> musics = new ArrayList<>();
        try {
            FileJson mf = dfs.searchFile("music");
            var pages = mf.getPages();
            var threads = new LinkedList<Thread>();

            for (var page : pages) {
                ArrayList<Long> guids = page.getGuids();
                for(int i = 0; i < guids.size(); i++){
                    ChordMessageInterface peer = dfs.getChord().locateSuccessor(guids.get(i));
                    Long guidOfPage = guids.get(i);
                    if(peer != null){
                        threads.add(new Thread(() -> {
                            try {
                                String json = peer.search(guidOfPage, query);
                                sem.acquire();
                                musics.addAll(Arrays.asList(gson.fromJson(json, Music[].class)));
                                sem.release();
                            } catch (RemoteException e) {
                                System.err.println("Error occurred while searching for song in chord");
                            } catch (InterruptedException e) {
                                System.err.println(e.getMessage());
                            }
                        }));
                        threads.getLast().start();
                        i = 3;
                    }
                }
            }
            for (Thread t : threads) {
                t.join();
            }

            // Sort music by song's names
            musics.sort(Comparator.comparing(a -> a.getSong().getTitle()));
        } catch (RemoteException e) {
            System.err.println("Error occurred while accessing resources from chord");
        } catch (InterruptedException e) {
            System.err.println("Thread execution timed out.");
        }

        var toIndex = Integer.min(end, musics.size());
        listSize = musics.size();
        return musics.subList(start, toIndex).toArray(new Music[0]);
    }
}
