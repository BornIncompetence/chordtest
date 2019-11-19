package com.cecs.DFS;

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

        public PagesJson(){
            this.guid = (long) 0;
            this.size = (long) 0;
            this.createTS = "0";
            this.readTS = "0";
            this.writeTS = "0";
            this.referenceCount = 0;
        }
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
        Long size; // Total size of the file, calculated from adding each part from every node
        ArrayList<PagesJson> pages;
        String creationTS;
        String readTS;
        String writeTS;
        int numOfPages;
        int maxPageSize; // The largest size of a page of the file

        public FileJson() {
            this.size = (long) 0;
            pages = new ArrayList<PagesJson>();
            creationTS = LocalDateTime.now().toString();
            readTS = "0";
            writeTS = "0";
            numOfPages = 0;
            maxPageSize = 0;
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
        public PagesJson getPage(int index){
            return this.pages.get(index);
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
        public void setReadTS(String readTS){
            this.readTS = readTS;
        }
        public void setWriteTS(String writeTS){
            this.writeTS = writeTS;
        }
        public void compareAndSetMaxPageSize(int newSize){
            if(this.maxPageSize < newSize){
                this.maxPageSize = newSize;
            }
        }
        public void addNewPage(PagesJson newPageToAdd){
            this.pages.add(newPageToAdd);
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
        FilesJson metadata = this.readMetaData();
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(oldName)){
                metadata.getFile(i).setName(newName);
            }
        }
        writeMetaData(metadata);
    }

    /**
     * List the files in the system
     *
     * @param filename Name of the file
     */
    public String lists() throws Exception {
        FilesJson fileJson = readMetaData();
        String listOfFiles = "";
        for(int i = 0; i < fileJson.getNumOfFilesInMetadata(); i++){
            listOfFiles = listOfFiles + fileJson.getFile(i).getName() + "\n";
        }
        return listOfFiles;
    }

    /**
     * create an empty file
     * Must run this function before appending any files
     *
     * @param filename Name of the file
     */
    public void create(String fileName) throws Exception {
        FilesJson metadata = this.readMetaData();
        FileJson newFile = new FileJson();
        newFile.setName(fileName);
        metadata.addFile(newFile);
        writeMetaData(metadata);
    }

    /**
     * delete file
     *
     * @param filename Name of the file
     */
    public void delete(String fileName) throws Exception {
        FilesJson metadata = this.readMetaData();
        metadata.removeFile(fileName);
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(fileName)){
                for(int j = 0; j < metadata.getFile(i).getNumOfPages(); j++){
                    Long guidOfPageJsonToDelete = metadata.getFile(i).getPage(j).getGuid();
                    ChordMessageInterface nodeThatHostsFile = chord.locateSuccessor(guidOfPageJsonToDelete);
                    nodeThatHostsFile.delete(guidOfPageJsonToDelete);
                }
            }
        }
        writeMetaData(metadata);
    }

    /**
     * Read block pageNumber of fileName
     *
     * @param filename   Name of the file
     * @param pageNumber number of block.
     */
    public RemoteInputFileStream read(String fileName, int pageNumber) throws Exception {
        FilesJson metadata = this.readMetaData();
        PagesJson pagesJson = null;
        RemoteInputFileStream rifs = null;
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(fileName)){
                System.out.println("found file");
                pagesJson = metadata.getFile(i).getPage(pageNumber);
                metadata.getFile(i).setReadTS(LocalDateTime.now().toString());
                ChordMessageInterface peer = chord.locateSuccessor(pagesJson.getGuid());
                rifs = peer.get(pagesJson.getGuid());
            }
            writeMetaData(metadata);
        }
        System.out.println(rifs);
        return rifs;
    }

    /**
     * Add a page to the file
     *
     * @param filename Name of the file
     * @param data     RemoteInputStream.
     */
    public void append(String filename, RemoteInputFileStream data) throws Exception {
        FilesJson metadata = this.readMetaData();
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){ //for loop to loop through each file in metadata
            if(metadata.getFile(i).getName().equals(filename)){ //found the correct file to append a page 
                //code to update the metadata of the specified file
                metadata.getFile(i).setSize(metadata.getFile(i).getSize() + (long) data.available());
                metadata.getFile(i).setReadTS(LocalDateTime.now().toString());
                metadata.getFile(i).setWriteTS(LocalDateTime.now().toString());
                metadata.getFile(i).setNumOfPages(metadata.getFile(i).getNumOfPages() + 1);
                metadata.getFile(i).compareAndSetMaxPageSize(data.available());
                //code to add to file to chord
                Long guidOfNewFile = md5(filename + LocalDateTime.now().toString());
                ChordMessageInterface nodeToHostFile = chord.locateSuccessor(guidOfNewFile);
                nodeToHostFile.put(guidOfNewFile, data);
                PagesJson newPageToAdd = new PagesJson(guidOfNewFile, (long) data.available(), LocalDateTime.now().toString(), 
                                                       LocalDateTime.now().toString(), LocalDateTime.now().toString(), 0);
                metadata.getFile(i).addNewPage(newPageToAdd);
            }
        }
        writeMetaData(metadata); //save changes to metadata file
    }

    /**
     * Reads the first page of the file
     * 
     * @param fileName   Name of the file
     * @return           First index of the pages in the file
     * @throws Exception
     */
    public RemoteInputFileStream head(String fileName) throws Exception{
        FilesJson metadata = this.readMetaData();
        PagesJson pagesJson = null;
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(fileName)){
                pagesJson = metadata.getFile(i).getPage(0);
                metadata.getFile(i).setReadTS(LocalDateTime.now().toString());
            }
        }
        writeMetaData(metadata);
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);
        RemoteInputFileStream blockData = peer.get(pagesJson.getGuid());
        return blockData;
    }

    /**
     * Reads the last page of the file
     * 
     * @param filename   Name of the file
     * @return           Last index of the pages in the file
     * @throws Exception
     */
    public RemoteInputFileStream tail(String filename) throws Exception{
        FilesJson metadata = this.readMetaData();
        PagesJson pagesJson = null;
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(filename)){
                pagesJson = metadata.getFile(i).getPage(metadata.getFile(i).getNumOfPages() - 1);
                metadata.getFile(i).setReadTS(LocalDateTime.now().toString());
            }
        }
        writeMetaData(metadata);
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);
        RemoteInputFileStream blockData = peer.get(pagesJson.getGuid());
        return blockData;
    }

	public int GetNumberOfPagesForFile(String filename) throws Exception {
		FilesJson metadata = this.readMetaData();
        int numberOfPagesInFile = 0;
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(filename)){
                numberOfPagesInFile = metadata.getFile(i).getNumOfPages();
            }
        }
        return numberOfPagesInFile;
    }
    
    public byte[] GetSong(String filename, long offset, int fragmentSize) throws Exception {
        FilesJson metadata = this.readMetaData();
        byte[] byteArrayOfSong = null;
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(filename)){
                System.out.println("Found song");
                Long guidOfSong = metadata.getFile(i).getPage(0).getGuid();
                ChordMessageInterface nodeToHostFile = chord.locateSuccessor(guidOfSong);
                byteArrayOfSong = nodeToHostFile.get(guidOfSong, offset, fragmentSize);
            }
        }
        return byteArrayOfSong;
    } 

    public int getSongSize(String filename) throws Exception {
        FilesJson metadata = this.readMetaData();
        int size = 0;
        for(int i = 0; i < metadata.getNumOfFilesInMetadata(); i++){
            if(metadata.getFile(i).getName().equals(filename)){
                size = metadata.getFile(i).getSize().intValue();
            }
        }
        return size;
    }
}
