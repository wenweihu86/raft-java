package com.github.wenweihu86.raft.storage;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;

/**
 * Created by wenweihu86 on 2017/7/2.
 */
public class SnapshotTest {

    @Test
    public void testReadSnapshotDataFiles() throws IOException {
        String raftDataDir = "./data";
        File file = new File("./data/message");
        file.mkdirs();
        File file1 = new File("./data/message/queue1.txt");
        file1.createNewFile();
        File file2 = new File("./data/message/queue2.txt");
        file2.createNewFile();

        File snapshotFile = new File("./data/snapshot");
        snapshotFile.mkdirs();
        Path link = FileSystems.getDefault().getPath("./data/snapshot/data");
        Path target = FileSystems.getDefault().getPath("./data/message").toRealPath();
        Files.createSymbolicLink(link, target);

        Snapshot snapshot = new Snapshot(raftDataDir);
        TreeMap<String, Snapshot.SnapshotDataFile> snapshotFileMap = snapshot.openSnapshotDataFiles();
        System.out.println(snapshotFileMap.keySet());
        Assert.assertTrue(snapshotFileMap.size() == 2);
        Assert.assertTrue(snapshotFileMap.firstKey().equals("queue1.txt"));

        Files.delete(link);
        FileUtils.deleteDirectory(new File(raftDataDir));
    }
}
