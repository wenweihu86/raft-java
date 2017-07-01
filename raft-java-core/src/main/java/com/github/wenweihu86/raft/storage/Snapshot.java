package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wenweihu86 on 2017/5/6.
 */
public class Snapshot {

    public class SnapshotDataFile {
        public String fileName;
        public RandomAccessFile randomAccessFile;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private String snapshotDir = RaftOptions.dataDir + File.separator + "snapshot";
    private RaftMessage.SnapshotMetaData metaData;
    private TreeMap<String, SnapshotDataFile> snapshotDataFileMap;
    private AtomicBoolean isInSnapshot = new AtomicBoolean(false);
    private Lock lock = new ReentrantLock();

    public Snapshot() {
        String snapshotDataDir = snapshotDir + File.separator + "data";
        File file = new File(snapshotDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void reload() {
        if (snapshotDataFileMap != null) {
            for (SnapshotDataFile file : snapshotDataFileMap.values()) {
                RaftFileUtils.closeFile(file.randomAccessFile);
            }
        }
        this.snapshotDataFileMap = readSnapshotDataFiles();
        metaData = this.readMetaData();
        if (metaData == null) {
            if (snapshotDataFileMap.size() > 0) {
                LOG.error("No readable metadata file but found snapshot in {}", snapshotDir);
                throw new RuntimeException("No readable metadata file but found snapshot");
            }
            metaData = RaftMessage.SnapshotMetaData.newBuilder().build();
            snapshotDataFileMap = new TreeMap<>();
        }
    }

    public TreeMap<String, SnapshotDataFile> readSnapshotDataFiles() {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();
        String snapshotDataDir = snapshotDir + File.separator + "data";
        List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(snapshotDataDir);
        for (String fileName : fileNames) {
            RandomAccessFile randomAccessFile = RaftFileUtils.openFile(snapshotDataDir, fileName, "r");
            SnapshotDataFile snapshotFile = new SnapshotDataFile();
            snapshotFile.fileName = fileName;
            snapshotFile.randomAccessFile = randomAccessFile;
            snapshotDataFileMap.put(fileName, snapshotFile);
        }
        return snapshotDataFileMap;
    }

    public RaftMessage.SnapshotMetaData readMetaData() {
        String fileName = snapshotDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            RaftMessage.SnapshotMetaData metadata = RaftFileUtils.readProtoFromFile(
                    randomAccessFile, RaftMessage.SnapshotMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    public void updateMetaData(String dir,
                               Long lastIncludedIndex,
                               Long lastIncludedTerm,
                               RaftMessage.Configuration configuration) {
        RaftMessage.SnapshotMetaData snapshotMetaData = RaftMessage.SnapshotMetaData.newBuilder()
                .setLastIncludedIndex(lastIncludedIndex)
                .setLastIncludedTerm(lastIncludedTerm)
                .setConfiguration(configuration).build();
        String snapshotMetaFile = dir + File.separator + "metadata";
        RandomAccessFile randomAccessFile = null;
        try {
            File dirFile = new File(dir);
            if (!dirFile.exists()) {
                dirFile.mkdirs();
            }

            File file = new File(snapshotMetaFile);
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
            file.createNewFile();
            randomAccessFile = new RandomAccessFile(file, "rw");
            RaftFileUtils.writeProtoToFile(randomAccessFile, snapshotMetaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", snapshotMetaFile);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
        }
    }

    public RaftMessage.SnapshotMetaData getMetaData() {
        return metaData;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public TreeMap<String, SnapshotDataFile> getSnapshotDataFileMap() {
        return snapshotDataFileMap;
    }

    public AtomicBoolean getIsInSnapshot() {
        return isInSnapshot;
    }

    public void setIsInSnapshot(AtomicBoolean isInSnapshot) {
        this.isInSnapshot = isInSnapshot;
    }

    public Lock getLock() {
        return lock;
    }
}
