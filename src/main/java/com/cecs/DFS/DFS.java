package com.cecs.DFS;

import java.util.*;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.*;
import java.time.LocalDateTime;

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

public class DFS {

    public class PagesJson { // This might be the class that holds the pages of the music.json or users.json?
        long guid;
        long size;
        String createTS;
        String readTS;
        String writeTS;
        int referenceCount;

        public PagesJson(long guid, long size, String timestamp, int referenceCount) {
            this.guid = guid;
            this.size = size;
            this.createTS = timestamp;
            this.readTS = timestamp;
            this.writeTS = timestamp;
            this.referenceCount = referenceCount;
        }

        // getters
        public long getGuid() {
            return this.guid;
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
            filesJson = gson.fromJson(strMetaData, FilesJson.class);
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
            long deleteGuid = page.guid;
            ChordMessageInterface peer = chord.locateSuccessor(deleteGuid);
            peer.delete(deleteGuid);
        }
        metadata.removeFile(filename);
        writeMetaData(metadata);
    }

    /**
     * Read block pageNumber of filename
     *
     * @param filename   Name of the file
     * @param pageNumber number of block.
     */
    public RemoteInputFileStream read(String filename, int pageNumber) throws RemoteException {
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();

        PagesJson pagesJson = file.pages.get(pageNumber);
        ChordMessageInterface peer = chord.locateSuccessor(pagesJson.guid);
        RemoteInputFileStream rifs = peer.get(pagesJson.guid);

        writeMetaData(metadata);
        System.out.println(rifs);
        return rifs;
    }

    /**
     * Add a page to the file
     *
     * @param filename Name of the file
     * @param data     RemoteInputStream.
     */
    public void append(String filename, RemoteInputFileStream data) throws RemoteException {
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);

        // Update metadata
        file.size += data.available();
        file.readTS = now();
        file.writeTS = now();
        file.compareAndSetMaxPageSize(data.available());

        // Add file to chord
        long guidOfNewFile = md5(filename + now());
        ChordMessageInterface nodeToHostFile = chord.locateSuccessor(guidOfNewFile);
        nodeToHostFile.put(guidOfNewFile, data); // Can possibly stall the entire program
        PagesJson newPage = new PagesJson(guidOfNewFile, data.available(), now(), 0);

        System.out.println("Adding file...");
        file.pages.add(newPage);
        writeMetaData(metadata);
    }

    /**
     * Reads the first page of the file
     * 
     * @param filename Name of the file
     * @return First index of the pages in the file
     */
    public RemoteInputFileStream head(String filename) throws RemoteException {
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();
        PagesJson pagesJson = file.pages.get(0);

        writeMetaData(metadata);
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);

        if (pagesJson != null) {
            return peer.get(pagesJson.guid);
        } else {
            throw new RemoteException("File not found!");
        }
    }

    /**
     * Reads the last page of the file
     * 
     * @param filename Name of the file
     * @return Last index of the pages in the file
     */
    public RemoteInputFileStream tail(String filename) throws RemoteException {
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();
        PagesJson pagesJson = file.pages.get(file.pages.size() - 1);

        writeMetaData(metadata);
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);
        return peer.get(pagesJson.guid);
    }

    public int getPageFilesize(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename).pages.size();
    }

    public byte[] getSong(String filename, long offset, int fragmentSize) throws RemoteException {
        long songGuid = this.readMetaData().getFile(filename).pages.get(0).guid;
        ChordMessageInterface peer = chord.locateSuccessor(songGuid);
        return peer.get(songGuid, offset, fragmentSize);
    }

    public long getSongSize(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename).size;
    }

    public FileJson searchFile(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename);
    }
}
