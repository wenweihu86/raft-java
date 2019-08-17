package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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
    private String snapshotDir;
    private RaftProto.SnapshotMetaData metaData;
    // 表示是否正在安装snapshot，leader向follower安装，leader和follower同时处于installSnapshot状态
    private AtomicBoolean isInstallSnapshot = new AtomicBoolean(false);
    // 表示节点自己是否在对状态机做snapshot
    private AtomicBoolean isTakeSnapshot = new AtomicBoolean(false);
    private Lock lock = new ReentrantLock();

    public Snapshot(String raftDataDir) {
        this.snapshotDir = raftDataDir + File.separator + "snapshot";
        String snapshotDataDir = snapshotDir + File.separator + "data";
        File file = new File(snapshotDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void reload() {
        metaData = this.readMetaData();
        if (metaData == null) {
            metaData = RaftProto.SnapshotMetaData.newBuilder().build();
        }
    }

    /**
     * 打开snapshot data目录下的文件，
     * 如果是软链接，需要打开实际文件句柄
     * @return 文件名以及文件句柄map
     */
    public TreeMap<String, SnapshotDataFile> openSnapshotDataFiles() {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();
        String snapshotDataDir = snapshotDir + File.separator + "data";
        try {
            Path snapshotDataPath = FileSystems.getDefault().getPath(snapshotDataDir);
            snapshotDataPath = snapshotDataPath.toRealPath();
            snapshotDataDir = snapshotDataPath.toString();
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(snapshotDataDir, snapshotDataDir);
            for (String fileName : fileNames) {
                RandomAccessFile randomAccessFile = RaftFileUtils.openFile(snapshotDataDir, fileName, "r");
                SnapshotDataFile snapshotFile = new SnapshotDataFile();
                snapshotFile.fileName = fileName;
                snapshotFile.randomAccessFile = randomAccessFile;
                snapshotDataFileMap.put(fileName, snapshotFile);
            }
        } catch (IOException ex) {
            LOG.warn("readSnapshotDataFiles exception:", ex);
            throw new RuntimeException(ex);
        }
        return snapshotDataFileMap;
    }

    public void closeSnapshotDataFiles(TreeMap<String, SnapshotDataFile> snapshotDataFileMap) {
        for (Map.Entry<String, SnapshotDataFile> entry : snapshotDataFileMap.entrySet()) {
            try {
                entry.getValue().randomAccessFile.close();
            } catch (IOException ex) {
                LOG.warn("close snapshot files exception:", ex);
            }
        }
    }

    public RaftProto.SnapshotMetaData readMetaData() {
        String fileName = snapshotDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            RaftProto.SnapshotMetaData metadata = RaftFileUtils.readProtoFromFile(
                    randomAccessFile, RaftProto.SnapshotMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    public void updateMetaData(String dir,
                               Long lastIncludedIndex,
                               Long lastIncludedTerm,
                               RaftProto.Configuration configuration) {
        RaftProto.SnapshotMetaData snapshotMetaData = RaftProto.SnapshotMetaData.newBuilder()
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

    public RaftProto.SnapshotMetaData getMetaData() {
        return metaData;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public AtomicBoolean getIsInstallSnapshot() {
        return isInstallSnapshot;
    }

    public AtomicBoolean getIsTakeSnapshot() {
        return isTakeSnapshot;
    }

    public Lock getLock() {
        return lock;
    }
}
