package com.cecs.DFS;

import java.util.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.*;
import java.time.LocalDateTime;

import com.cecs.Models.Transaction;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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

public class DFS implements AtomicCommitInterface {

    public class PagesJson { // This might be the class that holds the pages of the music.json or users.json?
        private ArrayList<Long> guids;
        long size;
        public ArrayList<String> createTS;
        public ArrayList<String> readTS;
        public ArrayList<String> writeTS;
        int referenceCount;

        public PagesJson(ArrayList<Long> guids, long size, ArrayList<String> timestamp, int referenceCount) {
            this.guids = guids;
            this.size = size;
            this.createTS = timestamp;
            this.readTS = timestamp;
            this.writeTS = timestamp;
            this.referenceCount = referenceCount;
        }

        // getters
        public ArrayList<Long> getGuids() {
            return this.guids;
        }
    }

    // Structure for all the files that will be listed in metadata? eg imperial.mp3
    // or song files
    public class FileJson {
        String name;
        long size; // Total size of the file, calculated from adding each part from every node
        ArrayList<PagesJson> pages;
        String creationTS;
        String readTS;
        String writeTS;
        int maxPageSize; // The largest size of a page of the file

        public FileJson() {
            this.size = 0L;
            pages = new ArrayList<>();
            creationTS = now();
            readTS = "0";
            writeTS = "0";
            maxPageSize = 0;
        }

        // getters
        public String getName() {
            return this.name;
        }

        public long getSize() {
            return this.size;
        }

        public List<PagesJson> getPages() {
            return this.pages;
        }

        // setters
        public void setName(String newName) {
            this.name = newName;
        }

        public void setSize(long newSize) {
            this.size = newSize;
        }

        public void setReadTS(String readTS) {
            this.readTS = readTS;
        }

        public void setWriteTS(String writeTS) {
            this.writeTS = writeTS;
        }

        public void compareAndSetMaxPageSize(int newSize) {
            if (this.maxPageSize < newSize) {
                this.maxPageSize = newSize;
            }
        }

        public void addNewPage(PagesJson newPageToAdd) {
            this.pages.add(newPageToAdd);
        }
    };

    public class FilesJson {// This is for the entire metadata file?
        ArrayList<FileJson> files;

        public FilesJson() {
            files = new ArrayList<>();
        }

        // getters
        public FileJson getFile(int index) {
            return files.get(index);
        }

        public int getNumOfFilesInMetadata() {
            return files.size();
        }

        public FileJson getFile(String filename) throws RemoteException {
            var file = files.stream().filter(fileJson -> fileJson.name.equals(filename)).findFirst();
            if (file.isEmpty()) {
                throw new RemoteException("File" + filename + "not found");
            }
            return file.get();
        }

        public boolean doesFileExist(String filename) {
            for (var file : files) {
                if (file.name.equals(filename)) {
                    return true;
                }
            }
            return false;
        }

        // setters
        public void addFile(FileJson fileToAdd) {
            files.add(fileToAdd);
        }

        public void removeFile(String filename) {
            for (var file : files) {
                if (file.name.equals(filename)) {
                    files.remove(file);
                    break;
                }
            }
        }
    }

    int port;
    Chord chord;
    Gson gson;

    public ChordMessageInterface getChord() {
        return chord;
    }

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

    /**
     * Gets the current time
     * 
     * @return The time as a string
     */
    private String now() {
        return LocalDateTime.now().toString();
    }

    public DFS(int port) throws IOException {
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.port = port;
        long guid = md5("" + port);
        this.chord = new Chord(port, guid);
        Files.createDirectories(Paths.get(guid + "/repository"));
        Files.createDirectories(Paths.get(guid + "/tmp"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> chord.leave()));
    }

    /**
     * Join the chord
     */
    public void join(String ip, int port) {
        chord.joinRing(ip, port);
        chord.print();
    }

    /**
     * leave the chord
     */
    public void leave() {
        chord.leave();
    }

    /**
     * print the status of the peer in the chord
     */
    public void print() {
        chord.print();
    }

    /**
     * Read the metadata from the chord
     */
    public FilesJson readMetaData() {
        FilesJson filesJson;
        try {
            long guid = md5("Metadata");
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            RemoteInputFileStream rawMetadata = peer.get(guid);
            rawMetadata.connect();
            Scanner scan = new Scanner(rawMetadata);
            scan.useDelimiter("\\A");
            String strMetaData = scan.next();
            System.out.println(strMetaData);
            filesJson = gson.fromJson(strMetaData, FilesJson.class);
            scan.close();
        } catch (RemoteException | NoSuchElementException e) {
            System.out.println("Creating new file!");
            filesJson = new FilesJson();
        }
        return filesJson;
    }

    /**
     * writeMetaData write the metadata back to the chord
     *
     */
    public void writeMetaData(FilesJson filesJson) throws RemoteException {
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);

        peer.put(guid, gson.toJson(filesJson));
    }

    /**
     * Change Name
     *
     */
    public void move(String oldName, String newName) throws RemoteException {
        FilesJson metadata = this.readMetaData();
        metadata.getFile(oldName).name = newName;
        writeMetaData(metadata);
    }

    /**
     * List the files in the system
     */
    public String listFiles() {
        FilesJson metadata = readMetaData();
        var listOfFiles = new StringBuilder();
        for (var file : metadata.files) {
            listOfFiles.append(file.name).append("\n");
        }
        return listOfFiles.toString();
    }

    /**
     * create an empty file
     * <p>
     * Must run this function before appending any files
     *
     * @param filename Name of the file
     */
    public void create(String filename) throws RemoteException {
        FilesJson metadata = this.readMetaData();
        FileJson newFile = new FileJson();
        newFile.setName(filename);
        metadata.addFile(newFile);
        writeMetaData(metadata);
    }

    /**
     * delete file
     *
     * @param filename Name of the file
     */
    public void delete(String filename) throws RemoteException {
        FilesJson metadata = this.readMetaData();
        for (var page : metadata.getFile(filename).pages) {
            ArrayList<Long> pageGuids = page.getGuids();
            for (int i = 0; i < pageGuids.size(); i++) {
                long deleteGuid = pageGuids.get(i);
                ChordMessageInterface peer = chord.locateSuccessor(deleteGuid);
                peer.delete(deleteGuid);
            }
        }
        metadata.removeFile(filename);
        writeMetaData(metadata);
    }

    /**
     * Read block pageNumber of filename
     *
     * @param filename   Name of the file
     * @param pageNumber number of block.
     * @throws RemoteException
     */
    public RemoteInputFileStream read(String filename, int pageNumber) throws RemoteException {
        RemoteInputFileStream rifs = null;
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();

        PagesJson pagesJson = file.pages.get(pageNumber);
        ArrayList<Long> guidsOfPage = pagesJson.guids;
        for (int i = 0; i < guidsOfPage.size(); i++) {
            ChordMessageInterface peer = chord.locateSuccessor(guidsOfPage.get(i));
            if (peer != null) {
                rifs = peer.get(guidsOfPage.get(i));
                pagesJson.readTS.set(i, now());
                writeMetaData(metadata);
                System.out.println(rifs);
                i = 3;
            }
        }
        return rifs;
    }

    /**
     * Add a page to the file
     *
     * @param filename Name of the file
     * @param data     RemoteInputStream.
     * @throws IOException
     */
    public void append(String filename, String filepath) throws IOException {
        FilesJson metadata = this.readMetaData();
        FileJson file = metadata.getFile(filename);

        // Add file to chord
        ArrayList<Long> fileGuids = new ArrayList<Long>();
        ArrayList<String> timestamps = new ArrayList<String>();
        String ts = now();
        for (int i = 0; i < 3; i++) {
            RemoteInputFileStream rifs = new RemoteInputFileStream(filepath);
            long guidOfNewFile = md5(filename + i + now());
            System.out.println("Page created with guid: " + guidOfNewFile);
            fileGuids.add(guidOfNewFile);
            timestamps.add(ts);
            ChordMessageInterface nodeToHostFile = chord.locateSuccessor(guidOfNewFile);
            System.out.println("Adding file...");
            nodeToHostFile.put(guidOfNewFile, rifs);            
        }
        // Update metadata
        RemoteInputFileStream rifs = new RemoteInputFileStream(filepath);
        file.size += rifs.available();
        file.readTS = now();
        file.writeTS = now();
        file.compareAndSetMaxPageSize(rifs.available());

        PagesJson newPage = new PagesJson(fileGuids, rifs.available(), timestamps, 0);
        
        file.pages.add(newPage);
        writeMetaData(metadata);
        rifs.close();
    }

    /**
     * Reads the first page of the file
     * 
     * @param filename Name of the file
     * @return First index of the pages in the file
     */
    public RemoteInputFileStream head(String filename) throws RemoteException {
        RemoteInputFileStream rifs = null;
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();

        PagesJson pagesJson = file.pages.get(0);

        ArrayList<Long> guidsOfPage = pagesJson.guids;
        for(int i = 0; i < guidsOfPage.size(); i++){
            ChordMessageInterface peer = chord.locateSuccessor(guidsOfPage.get(i));
            if(peer != null){
                rifs = peer.get(guidsOfPage.get(i));
                writeMetaData(metadata);
                System.out.println(rifs);
                i = 3;
            }
        }
        return rifs;
    }

    /**
     * Reads the last page of the file
     * 
     * @param filename Name of the file
     * @return Last index of the pages in the file
     */
    public RemoteInputFileStream tail(String filename) throws RemoteException {
        RemoteInputFileStream rifs = null;
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();
        PagesJson pagesJson = file.pages.get(file.pages.size() - 1);

        ArrayList<Long> guidsOfPage = pagesJson.guids;
        for(int i = 0; i < guidsOfPage.size(); i++){
            ChordMessageInterface peer = chord.locateSuccessor(guidsOfPage.get(i));
            if(peer != null){
                rifs = peer.get(guidsOfPage.get(i));
                writeMetaData(metadata);
                System.out.println(rifs);
                i = 3;
            }
        }
        return rifs;
    }

    public int getPageFilesize(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename).pages.size();
    }

    public byte[] getSong(String filename, long offset, int fragmentSize) throws RemoteException {
        byte[] songBytes = null;
        ArrayList<Long> guidsOfPage = this.readMetaData().getFile(filename).pages.get(0).guids;
        for(int i = 0; i < guidsOfPage.size(); i++){
            ChordMessageInterface peer = chord.locateSuccessor(guidsOfPage.get(i));
            if(peer != null){
                songBytes = peer.get(guidsOfPage.get(i), offset, fragmentSize);
                i = 3;
            }
        }
        return songBytes;
    }

    public long getSongSize(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename).size;
    }

    public FileJson searchFile(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename);
    }

    public void pull(String filename, int pageIndex){
        String directoryFilePath = (String.valueOf(port) + "_dir");
        File f = new File(directoryFilePath);
        try{
            if(!f.exists()){
                if(f.mkdir()){
                    copyFileToTempDirectory(filename, pageIndex, directoryFilePath);
                    Transaction transactionToPull = new Transaction(filename, pageIndex);
                    var transactionJson = gson.toJson(transactionToPull);
                    var writer = new FileWriter(new File(directoryFilePath, "transaction.json"));
                    writer.write(transactionJson);
                    writer.close();
                }
                else{
                    System.out.println("Directory is not created");
                }
            }
            copyFileToTempDirectory(filename, pageIndex, directoryFilePath);
            Transaction transactionToPull = new Transaction(filename, pageIndex);
            var transactionJson = gson.toJson(transactionToPull);
            var writer = new FileWriter(new File(directoryFilePath, "transaction.json"));
            writer.write(transactionJson);
            writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void push() throws IOException {
        String directoryFilePath = (String.valueOf(port) + "_dir");
        var file = new File(directoryFilePath + "/transaction.json");
        var reader = new FileReader(file, StandardCharsets.UTF_8);
        Transaction transactionToPush = gson.fromJson(reader, Transaction.class);
        if(canCommit(transactionToPush)){
            commit(transactionToPush);
            System.out.println("Push Successful");
        }else{
            System.out.println("Unable to push, latest pull is older than age of file");
        }
    }
    /*
    Checks to see if each node is allowed to commit by checking the read timestamp of each guid of each page
    */
    @Override
    public Boolean canCommit(Transaction trans) throws RemoteException {
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(trans.fileName);

        PagesJson pageOfFile = file.pages.get(trans.pageIndex);
        LocalDateTime transactionTS = LocalDateTime.parse(trans.ts);
        for(int i = 0; i < pageOfFile.readTS.size(); i++){
            LocalDateTime lastReadTS = LocalDateTime.parse(pageOfFile.readTS.get(i));
            if(lastReadTS.isAfter(transactionTS)){
                return false;
            }
        }
        return true;
    }

    @Override
    public void commit(Transaction trans) throws IOException {
        String directoryFilePath = (String.valueOf(port) + "_dir");
        FilesJson metadata = this.readMetaData();
        FileJson file = metadata.getFile(trans.fileName);
        file.pages.get(trans.pageIndex);
        // Delete old file in chord
        for(int i = 0; i < file.pages.get(trans.pageIndex).guids.size(); i++){
            ChordMessageInterface nodeToHostFile = chord.locateSuccessor(file.pages.get(trans.pageIndex).guids.get(i));
            nodeToHostFile.delete(file.pages.get(trans.pageIndex).guids.get(i));
        }
        file.pages.get(trans.pageIndex).guids.clear();
        // Put new file in chord
        String now = now();
        for(int i = 0; i < 3; i++){
            RemoteInputFileStream rifs = new RemoteInputFileStream(directoryFilePath + "/" + trans.fileName);
            Long guidOfFile = md5(trans.fileName + i + now());
            System.out.println("Page created with guid: " + guidOfFile);
            ChordMessageInterface nodeToHostFile = chord.locateSuccessor(guidOfFile);
            file.pages.get(trans.pageIndex).guids.add(guidOfFile);
            file.pages.get(trans.pageIndex).createTS.set(i, now); //Update the timestamps of guid
            file.pages.get(trans.pageIndex).readTS.set(i, now);
            file.pages.get(trans.pageIndex).writeTS.set(i, now);
            nodeToHostFile.put(guidOfFile, rifs);
        }
        RemoteInputFileStream rifs = new RemoteInputFileStream(directoryFilePath + "/" + trans.fileName);
        file.size = file.size - file.pages.get(trans.pageIndex).size;
        file.size = file.size + rifs.available();
        file.readTS = now();
        file.writeTS = now();
        file.compareAndSetMaxPageSize(rifs.available());
        rifs.close();
        writeMetaData(metadata);
    }

    public Boolean copyFileToTempDirectory(String filename, int pageIndex, String directory){
        try{
            RemoteInputFileStream rifs = this.read(filename, pageIndex);
            File targetFile = new File(directory + "/" + filename);
            rifs.connect();
            try{
                OutputStream outStream = new FileOutputStream(targetFile);
                while(rifs.available() > 0){
                    outStream.write(rifs.read());
                }
                outStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
}
