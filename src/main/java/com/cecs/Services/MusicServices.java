package com.cecs.Services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.cecs.DFS.DFS;
import com.cecs.DFS.DFS.FileJson;
import com.cecs.Models.Music;
import com.google.gson.Gson;


public class MusicServices {
    private Music[] library;
    //private HashMap<String, List<Music>> queries = new HashMap<>();
    public DFS dfs;
    public Gson gson = new Gson();

    public int listSize;
    public MusicServices(DFS dfs) {
        this.dfs = dfs;
        this.listSize = 0;
    }
    public int getListSize(){
        return listSize;
    }
    public Music[] loadChunk(int start, int end, String query) {
        System.out.println("run loadChunk");
        List<Music> list = new ArrayList<>();
        try {
            FileJson mf = dfs.searchFile("music");
            int numOfPages = mf.getNumOfPages();
            Thread[] threads = new Thread[numOfPages];
            Music[][] musics = new Music[numOfPages][];
            for(int i = 0; i < numOfPages; i++){              
                Long guid = mf.getPage(i).getGuid();
                var peer = dfs.getChord().locateSuccessor(guid);
                int idx = i;
                threads[idx] = new Thread(){
                    public void run(){
                        try { 
                            String json = peer.search(guid, query);
                            musics[idx] = gson.fromJson(json, Music[].class);                           
                        } catch (Exception e) {
                            //TODO: handle exception
                        }
                    }
                };
            }

            for(Thread t : threads){
                t.start();
                t.join();
            }
            for(Thread t : threads){
                t.join();
            }

            for(Music[] musicList : musics){
                list.addAll(Arrays.asList(musicList));
            }


        } catch (Exception e) {
            //TODO: handle exception
        }
        var toIndex = Integer.min(end, list.size());
        listSize = list.size();
        return list.subList(start, toIndex).toArray(new Music[0]);
    }
}
