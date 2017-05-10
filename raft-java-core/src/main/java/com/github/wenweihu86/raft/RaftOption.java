package com.github.wenweihu86.raft;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftOption {

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in electionTimeoutMs milliseconds
    public static int electionTimeoutMilliseconds = 1000;

    // A leader sends RPCs at least this often, even if there is no data to send
    public static int heartbeatPeriodMilliseconds = 500;

    // snapshot定时器执行间隔
    public static int snapshotPeriodSeconds = 60;
    // log entry大小达到snapshotMinLogSize，才做snapshot
    public static int snapshotMinLogSize = 100 * 1024 * 1024;

    public static int maxLogEntriesPerRequest = 5000;

    // A snapshot saving would be triggered every snapshotInterval seconds
    public static int snapshotInterval = 3600;

    // 单个segment文件大小，默认100m
    public static int maxSegmentFileSize = 100 * 1000 * 1000;

    // raft的log和snapshot父目录，绝对路径
    public static String dataDir = System.getProperty("com.wenweihu86.raft.data.dir");
}
