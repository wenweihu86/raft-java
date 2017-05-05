package com.wenweihu86.raft;

import java.io.File;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftOption {

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in electionTimeoutMs milliseconds
    public static int electionTimeoutMilliseconds = 1000;

    // A leader sends RPCs at least this often, even if there is no data to send
    public static int heartbeatPeriodMilliseconds = 500;

    public static int maxLogEntriesPerRequest = 5000;

    // A snapshot saving would be triggered every snapshotInterval seconds
    public static int snapshotInterval = 3600;

    // 单个segment文件大小，默认100m
    public static int maxSegmentFileSize = 100 * 1000 * 1000;

    // data目录，绝对路径
    public static String dataDir = System.getProperty("com.wenweihu86.raft.home")
            + File.pathSeparator + "data";
}
