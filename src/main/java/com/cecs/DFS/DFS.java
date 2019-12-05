package com.cecs.DFS;

import java.util.*;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.io.IOException;
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
        String createTS;
        String readTS;
        String writeTS;
        int referenceCount;

        public PagesJson(ArrayList<Long> guids, long size, String timestamp, int referenceCount) {
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
        // TODO: add vote for consensus on transactions
        FilesJson metadata = this.readMetaData();
        for (var page : metadata.getFile(filename).pages) {
            for (var guid : page.guids) {
                chord.locateSuccessor(guid).delete(guid);
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
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);
        file.readTS = now();

        PagesJson pagesJson = file.pages.get(pageNumber);
        for (var guid : pagesJson.guids) {
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            if (peer != null) {
                RemoteInputFileStream rifs = peer.get(guid);
                writeMetaData(metadata);
                System.out.println(rifs);
                return rifs;
            }
        }
        return null;
    }

    /**
     * Add a page to the file
     *
     * @param filename Name of the file
     * @param data     RemoteInputStream.
     */
    public void append(String filename, RemoteInputFileStream data) throws RemoteException {
        // TODO: Vote on consensus
        FilesJson metadata = this.readMetaData();

        FileJson file = metadata.getFile(filename);

        // Update metadata
        file.size += data.available();
        file.readTS = now();
        file.writeTS = now();
        file.compareAndSetMaxPageSize(data.available());

        // Add file to chord
        ArrayList<Long> fileGuids = new ArrayList<Long>();
        for (int i = 0; i < 3; i++) {
            long guidOfNewFile = md5(filename + i + now());
            fileGuids.add(guidOfNewFile);
            ChordMessageInterface nodeToHostFile = chord.locateSuccessor(guidOfNewFile);
            nodeToHostFile.put(guidOfNewFile, data);
        }

        PagesJson newPage = new PagesJson(fileGuids, data.available(), now(), 0);
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

        for (var guid : pagesJson.guids) {
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            if (peer != null) {
                RemoteInputFileStream rifs = peer.get(guid);
                writeMetaData(metadata);
                System.out.println(rifs);
                return rifs;
            }
        }
        return null;
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

        for (var guid : pagesJson.guids) {
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            if (peer != null) {
                RemoteInputFileStream rifs = peer.get(guid);
                writeMetaData(metadata);
                System.out.println(rifs);
                return rifs;
            }
        }
        return null;
    }

    public int getPageFilesize(String filename) throws RemoteException {
        return this.readMetaData().getFile(filename).pages.size();
    }

    public byte[] getSong(String filename, long offset, int fragmentSize) throws RemoteException {
        byte[] songBytes = null;
        ArrayList<Long> guidsOfPage = this.readMetaData().getFile(filename).pages.get(0).guids;
        for (int i = 0; i < guidsOfPage.size(); i++) {
            ChordMessageInterface peer = chord.locateSuccessor(guidsOfPage.get(i));
            if (peer != null) {
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

    public void vote() {

    }

    public void pull() {

    }

    public void push(String filename, String operation){
        //pageindex = guid?
        Transaction transactionToPush = new Transaction("YES", operation, filename, pageIndex)
    }

    @Override
    public Boolean canCommit(Transaction trans) {
        // find each node that contains the page
        // tell each node to compare the timestamp of the page to the timestamp of the
        // transaction
        return null;
    }

    @Override
    public void commit(Transaction trans) {
        // forloop for each node that contains page
        // if timestamp of page is older than transaction, update the page with the
        // directory, update the timestamp

    }

    @Override
    public void abort(Transaction trans) {
        // forloop to check each node
        // if change has been committed (check if the timestamp has been updated on a
        // page)
        // copy contents of uncommitted node (timestamp of page in that node is less
        // than transaction) to this node

    }

    @Override
    public Boolean hasBeenCommitted(Transaction trans) {
        // forloop to check each node's page's timestamp and compare it to the
        // transaction timestamp
        // if page timestamp == transaction timestamp
        // return true
        return null;
    }

    @Override
    public Boolean getDecision(Transaction trans) {
        // for loop to ask each node to check their decision (canCommit?)
        return null;
    }
}
