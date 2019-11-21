package com.cecs.Services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.cecs.DFS.DFS;
import com.cecs.DFS.DFS.FileJson;
import com.cecs.Models.Music;
import com.google.gson.Gson;


public class MusicServices {
    public DFS dfs;
    public Gson gson = new Gson();
    private  Semaphore sem;
    public int listSize; // store current size of list
    public MusicServices(DFS dfs) {
        this.dfs = dfs;
        this.listSize = 0;
        this.sem = new Semaphore(1, true);
    }
    public int getListSize(){
        return listSize;
    }

    /*Load some part of the music.json */
    public Music[] loadChunk(int start, int end, String query) {
        System.out.println("run loadChunk");
        List<Music> musics = new ArrayList<>();
        try {
            FileJson mf = dfs.searchFile("music");
            int numOfPages = mf.getNumOfPages();
            Thread[] threads = new Thread[numOfPages];
            
            for(int i = 0; i < numOfPages; i++){              
                Long guid = mf.getPage(i).getGuid();
                var peer = dfs.getChord().locateSuccessor(guid);
                threads[i] = new Thread(){
                    public void run(){
                        try { 
                            String json = peer.search(guid, query);
                            sem.acquire();
                            musics.addAll(Arrays.asList(gson.fromJson(json, Music[].class)));      
                            sem.release();                 
                        } catch (Exception e) {
                            //TODO: handle exception
                            System.out.println(e);
                        }
                    }
                };
            }

            for(Thread t : threads){
                t.start();
                t.join();
            }
            
            //Sort music by song's names
            Collections.sort(musics, (a,b) -> {return a.getSong().getTitle().compareTo(b.getSong().getTitle());});
        } catch (Exception e) {
            //TODO: handle exception
            System.out.println(e);
        }

        var toIndex = Integer.min(end, musics.size());
        listSize = musics.size();
        return musics.subList(start, toIndex).toArray(new Music[0]);
    }
}
