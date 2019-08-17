package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.proto.RaftProto;
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
        String raftDataDir = "./data";
        SegmentedLog segmentedLog = new SegmentedLog(raftDataDir, 32);
        Assert.assertTrue(segmentedLog.getFirstLogIndex() == 1);

        List<RaftProto.LogEntry> entries = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            RaftProto.LogEntry entry = RaftProto.LogEntry.newBuilder()
                    .setData(ByteString.copyFrom(("testEntryData" + i).getBytes()))
                    .setType(RaftProto.EntryType.ENTRY_TYPE_DATA)
                    .setIndex(i)
                    .setTerm(i)
                    .build();
            entries.add(entry);
        }
        long lastLogIndex = segmentedLog.append(entries);
        Assert.assertTrue(lastLogIndex == 9);

        segmentedLog.truncatePrefix(5);
        FileUtils.deleteDirectory(new File(raftDataDir));
    }
}
