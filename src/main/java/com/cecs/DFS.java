package com.cecs;

import java.util.*;
import java.nio.file.*;
import java.math.BigInteger;
import java.security.*;
import java.time.LocalDateTime;

import com.google.gson.Gson;

/* JSON Format

{
    "file": [
        {
            "name":"MyFile",
            "size":128000000,
            "pages": [
                {
                    "guid":11,
                    "size":64000000
                },
                {
                    "guid":13,
                    "size":64000000
                }
            ]
        }
    ]
} 
*/

public class DFS {

    public class PagesJson { //This might be the class that holds the pages of the music.json or users.json?
        Long guid;
        Long size;
        String createTS;
        String readTS;
        String writeTS;
        int referenceCount;
        public PagesJson(Long guid, Long size, String createTS, String readTS, String writeTS, int referenceCount) {
            this.guid = guid;
            this.size = size;
            this.createTS = createTS;
            this.readTS = readTS;
            this.writeTS = writeTS;
            this.referenceCount = referenceCount;
        }
        // getters
        public Long getGuid(){
            return this.guid;
        }
        public Long getSize(){
            return this.size;
        }
        public String getCreateTS(){
            return this.createTS;
        }
        public String getReadTS(){
            return this.readTS;
        }
        public String getWriteTS(){
            return this.writeTS;
        }
        public int getReferenceCount(){
            return this.referenceCount;
        }

        // setters
        public void setGuid(Long guid){
            this.guid = guid;
        }
        public void setSize(Long size){
            this.size = size;
        }
        public void setCreateTS(String createTS){
            this.createTS = createTS;
        }
        public void setReadTS(String readTS){
            this.readTS = readTS;
        }
        public void setWriteTS(String writeTS){
            this.writeTS = writeTS;
        }

    };

    public class FileJson { //Structure for all the files that will be listed in metadata? eg imperial.mp3 or song files
        String name;
        Long size;
        ArrayList<PagesJson> pages;
        String creationTS;
        String readTS;
        String writeTS;
        int numOfPages;

        public FileJson() {
            this.size = (long) 0;
            creationTS = LocalDateTime.now().toString();
            readTS = "0";
            writeTS = "0";
            numOfPages = 0;
        }
        // getters
        public String getName(){
            return this.name;
        }
        public Long getSize(){
            return this.size;
        }
        public int getNumOfPages(){
            return this.numOfPages;
        }
        // setters
        public void setName(String newName){
            this.name = newName;
        }
        public void setSize(Long newSize){
            this.size = newSize;
        }
        public void setNumOfPages(int newNumOfPages){
            this.numOfPages = newNumOfPages;
        }
    };

    public class FilesJson {//This is for the entire metadata file?
        List<FileJson> file;
        public FilesJson() {
            file = new ArrayList<FileJson>();
        }

        // getters
        public FileJson getFile(int index){
            return file.get(index);
        }

        public int getNumOfFilesInMetadata(){
            return file.size();
        }
        
        public Boolean doesFileExist(String fileName){
            for(int i = 0; i < file.size(); i++){
                if(file.get(i).getName().equals(fileName)){
                    return true;
                }
            }
            return false;
        }
        // setters
        public void addFile(FileJson fileToAdd){
            file.add(fileToAdd);
        }
        
        public void removeFile(String fileName){
            for(int i = 0; i < file.size(); i++){
                if(file.get(i).getName().equals(fileName)){
                    file.remove(i);
                }
            }
        }
    };

    int port;
    Chord chord;

    private long md5(String objectName) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());
            BigInteger bigInt = new BigInteger(1, m.digest());
            return Math.abs(bigInt.longValue());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public DFS(int port) throws Exception {

        this.port = port;
        long guid = md5("" + port);
        chord = new Chord(port, guid);
        Files.createDirectories(Paths.get(guid + "/repository"));
        Files.createDirectories(Paths.get(guid + "/tmp"));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                chord.leave();
            }
        });

    }

    /**
     * Join the chord
     *
     */
    public void join(String Ip, int port) throws Exception {
        chord.joinRing(Ip, port);
        chord.print();
    }

    /**
     * leave the chord
     *
     */
    public void leave() throws Exception {
        chord.leave();
    }

    /**
     * print the status of the peer in the chord
     *
     */
    public void print() throws Exception {
        chord.print();
    }

    /**
     * readMetaData read the metadata from the chord
     *
     */
    public FilesJson readMetaData() throws Exception {
        FilesJson filesJson = null;
        try {
            Gson gson = new Gson();
            long guid = md5("Metadata");

            System.out.println("GUID " + guid);
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            RemoteInputFileStream metadataraw = peer.get(guid);
            metadataraw.connect();
            Scanner scan = new Scanner(metadataraw);
            scan.useDelimiter("\\A");
            String strMetaData = scan.next();
            System.out.println(strMetaData);
            filesJson = gson.fromJson(strMetaData, FilesJson.class);
        } catch (Exception ex) {
            filesJson = new FilesJson();
        }
        return filesJson;
    }

    /**
     * writeMetaData write the metadata back to the chord
     *
     */
    public void writeMetaData(FilesJson filesJson) throws Exception {
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);

        Gson gson = new Gson();
        peer.put(guid, gson.toJson(filesJson));
    }

    /**
     * Change Name
     *
     */
    public void move(String oldName, String newName) throws Exception {
        // TODO: Change the name in Metadata
        // Write Metadata
    }

    /**
     * List the files in the system
     *
     * @param filename Name of the file
     */
    public String lists() throws Exception {
        FilesJson fileJson = readMetaData();
        String listOfFiles = "";

        return listOfFiles;
    }

    /**
     * create an empty file
     *
     * @param filename Name of the file
     */
    public void create(String fileName) throws Exception {
        // TODO: Create the file fileName by adding a new entry to the Metadata
        // Write Metadata

    }

    /**
     * delete file
     *
     * @param filename Name of the file
     */
    public void delete(String fileName) throws Exception {

    }

    /**
     * Read block pageNumber of fileName
     *
     * @param filename   Name of the file
     * @param pageNumber number of block.
     */
    public RemoteInputFileStream read(String fileName, int pageNumber) throws Exception {
        return null;
    }

    /**
     * Add a page to the file
     *
     * @param filename Name of the file
     * @param data     RemoteInputStream.
     */
    public void append(String filename, RemoteInputFileStream data) throws Exception {

    }

}
