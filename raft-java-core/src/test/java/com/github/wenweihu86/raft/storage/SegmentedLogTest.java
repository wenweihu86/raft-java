package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/6/30.
 */
public class SegmentedLogTest {

    @Test
    public void testTruncateSuffix() throws IOException {
        RaftOptions.dataDir = "./data";
        RaftOptions.maxSegmentFileSize = 32;
        SegmentedLog segmentedLog = new SegmentedLog();
        Assert.assertTrue(segmentedLog.getFirstLogIndex() == 0);

        List<RaftMessage.LogEntry> entries = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            RaftMessage.LogEntry entry = RaftMessage.LogEntry.newBuilder()
                    .setData(ByteString.copyFrom(("testEntryData" + i).getBytes()))
                    .setType(RaftMessage.EntryType.ENTRY_TYPE_DATA)
                    .setIndex(i)
                    .setTerm(i)
                    .build();
            entries.add(entry);
        }
        long lastLogIndex = segmentedLog.append(entries);
        Assert.assertTrue(lastLogIndex == 9);

        segmentedLog.truncatePrefix(5);
        long firstLogIndex = segmentedLog.getFirstLogIndex();
        Assert.assertTrue(firstLogIndex == 5);

        FileUtils.deleteDirectory(new File(RaftOptions.dataDir));
    }
}
