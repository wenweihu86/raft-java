package com.github.wenweihu86.raft.util;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/7/3.
 */
public class RaftUtilsTest {

    @Test
    public void testGetSortedFilesInDirectory() throws IOException {
        File queue0 = new File("./data/message/example-topic/0");
        queue0.mkdirs();
        File segmentLog00 = new File("./data/message/example-topic/0/segment0");
        segmentLog00.createNewFile();
        File segmentLog01 = new File("./data/message/example-topic/0/segment1");
        segmentLog01.createNewFile();

        File queue1 = new File("./data/message/example-topic/1");
        queue1.mkdirs();
        File segmentLog12 = new File("./data/message/example-topic/1/segment2");
        segmentLog12.createNewFile();
        File segmentLog13 = new File("./data/message/example-topic/1/segment3");
        segmentLog13.createNewFile();

        List<String> files = RaftFileUtils.getSortedFilesInDirectory(
                "./data/message", "./data/message");
        System.out.println(files);
        Assert.assertTrue(files.size() == 4);
        Assert.assertTrue(files.contains("example-topic/0/segment0"));

        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }
}
