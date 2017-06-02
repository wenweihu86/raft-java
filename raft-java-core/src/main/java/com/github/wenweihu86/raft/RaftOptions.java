package com.github.wenweihu86.raft;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftOptions {

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in electionTimeoutMs milliseconds
    public static int electionTimeoutMilliseconds = 5000;

    // A leader sends RPCs at least this often, even if there is no data to send
    public static int heartbeatPeriodMilliseconds = 500;

    // snapshot定时器执行间隔
    public static int snapshotPeriodSeconds = 3600;
    // log entry大小达到snapshotMinLogSize，才做snapshot
    public static int snapshotMinLogSize = 100 * 1024 * 1024;
    public static int maxSnapshotBytesPerRequest = 500 * 1024; // 500k

    public static int maxLogEntriesPerRequest = 5000;

    // 单个segment文件大小，默认100m
    public static int maxSegmentFileSize = 100 * 1000 * 1000;

    // follower与leader差距在catchupMargin，才可以参与选举和提供服务
    public static long catchupMargin = 500;

    // replicate最大等待超时时间，单位ms
    public static long maxAwaitTimeout = 1000;

    // raft的log和snapshot父目录，绝对路径
    public static String dataDir = System.getProperty("com.github.wenweihu86.raft.data.dir");
}
