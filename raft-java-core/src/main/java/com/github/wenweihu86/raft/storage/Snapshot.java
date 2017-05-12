package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOption;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.List;
import java.util.TreeMap;

/**
 * Created by wenweihu86 on 2017/5/6.
 */
public class Snapshot {

    public class SnapshotDataFile {
        public String fileName;
        public RandomAccessFile randomAccessFile;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private String snapshotDir = RaftOption.dataDir + File.pathSeparator + "snapshot";
    private Raft.SnapshotMetaData metaData;
    private TreeMap<String, SnapshotDataFile> snapshotDataFileMap;

    public Snapshot() {
        File file = new File(snapshotDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        this.snapshotDataFileMap = readSnapshotDataFiles();
        metaData = this.readMetaData();
        if (metaData == null) {
            if (snapshotDataFileMap.size() > 0) {
                LOG.error("No readable metadata file but found snapshot in {}", snapshotDir);
                throw new RuntimeException("No readable metadata file but found snapshot");
            }
            metaData = Raft.SnapshotMetaData.newBuilder().build();
            snapshotDataFileMap = new TreeMap<>();
        }
    }

    public TreeMap<String, SnapshotDataFile> readSnapshotDataFiles() {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();
        String snapshotDataDir = snapshotDir + File.pathSeparator + "data";
        List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(snapshotDataDir);
        for (String fileName : fileNames) {
            RandomAccessFile randomAccessFile = RaftFileUtils.openFile(snapshotDir, fileName, "r");
            SnapshotDataFile snapshotFile = new SnapshotDataFile();
            snapshotFile.fileName = fileName;
            snapshotFile.randomAccessFile = randomAccessFile;
            snapshotDataFileMap.put(fileName, snapshotFile);
        }
        return snapshotDataFileMap;
    }

    public Raft.SnapshotMetaData readMetaData() {
        String fileName = snapshotDir + File.pathSeparator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            Raft.SnapshotMetaData metadata = RaftFileUtils.readProtoFromFile(
                    randomAccessFile, Raft.SnapshotMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    public void updateMetaData(String dir, Long lastIncludedIndex, Long lastIncludedTerm) {
        Raft.SnapshotMetaData snapshotMetaData = Raft.SnapshotMetaData.newBuilder()
                .setLastIncludedIndex(lastIncludedIndex)
                .setLastIncludedTerm(lastIncludedTerm).build();
        this.metaData = snapshotMetaData;
        RandomAccessFile randomAccessFile = null;
        String snapshotMetaFile = dir + File.pathSeparator + "metadata";
        try {
            File file = new File(snapshotMetaFile);
            if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            RaftFileUtils.writeProtoToFile(randomAccessFile, metaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", snapshotMetaFile);
        } finally {
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (Exception ex2) {
                    LOG.warn("close failed");
                }
            }
        }
    }

    public Raft.SnapshotMetaData getMetaData() {
        return metaData;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public TreeMap<String, SnapshotDataFile> getSnapshotDataFileMap() {
        return snapshotDataFileMap;
    }
}
