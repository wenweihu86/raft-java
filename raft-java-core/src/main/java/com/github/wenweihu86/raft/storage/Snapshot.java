package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOption;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/6.
 */
public class Snapshot {

    public class SnapshotFile {
        public String fileName;
        public RandomAccessFile randomAccessFile;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    private String snapshotDir = RaftOption.dataDir + File.pathSeparator + "snapshot";
    private Raft.SnapshotMetaData metaData;
    private List<SnapshotFile> snapshotFiles;

    public Snapshot() {
        File file = new File(snapshotDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        this.snapshotFiles = readSnapshotFiles();
        metaData = this.readMetaData();
        if (metaData == null) {
            if (snapshotFiles.size() > 0) {
                LOG.error("No readable metadata file but found snapshot in {}", snapshotDir);
                throw new RuntimeException("No readable metadata file but found snapshot");
            }
            metaData = Raft.SnapshotMetaData.newBuilder().build();
        }
    }

    public List<SnapshotFile> readSnapshotFiles() {
        List<SnapshotFile> snapshotFileList = new ArrayList<>();
        List<String> fileNames = FileUtil.getSortedFilesInDirectory(snapshotDir);
        for (String fileName : fileNames) {
            if (fileName.equals("metadata")) {
                continue;
            }
            RandomAccessFile randomAccessFile = FileUtil.openFile(snapshotDir, fileName, "r");
            SnapshotFile snapshotFile = new SnapshotFile();
            snapshotFile.fileName = fileName;
            snapshotFile.randomAccessFile = randomAccessFile;
            snapshotFileList.add(snapshotFile);
        }
        return snapshotFileList;
    }

    public Raft.SnapshotMetaData readMetaData() {
        String fileName = snapshotDir + File.pathSeparator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            Raft.SnapshotMetaData metadata = FileUtil.readProtoFromFile(
                    randomAccessFile, Raft.SnapshotMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    public Raft.SnapshotMetaData getMetaData() {
        return metaData;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }
}
