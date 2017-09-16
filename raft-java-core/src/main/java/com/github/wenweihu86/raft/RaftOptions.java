package com.github.wenweihu86.raft;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftOptions {

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in electionTimeoutMs milliseconds
    private int electionTimeoutMilliseconds = 5000;

    // A leader sends RPCs at least this often, even if there is no data to send
    private int heartbeatPeriodMilliseconds = 500;

    // snapshot定时器执行间隔
    private int snapshotPeriodSeconds = 3600;
    // log entry大小达到snapshotMinLogSize，才做snapshot
    private int snapshotMinLogSize = 100 * 1024 * 1024;
    private int maxSnapshotBytesPerRequest = 500 * 1024; // 500k

    private int maxLogEntriesPerRequest = 5000;

    // 单个segment文件大小，默认100m
    private int maxSegmentFileSize = 100 * 1000 * 1000;

    // follower与leader差距在catchupMargin，才可以参与选举和提供服务
    private long catchupMargin = 500;

    // replicate最大等待超时时间，单位ms
    private long maxAwaitTimeout = 1000;

    // 与其他节点进行同步、选主等操作的线程池大小
    private int raftConsensusThreadNum = 20;

    // raft的log和snapshot父目录，绝对路径
    private String dataDir = System.getProperty("com.github.wenweihu86.raft.data.dir");

    public int getElectionTimeoutMilliseconds() {
        return electionTimeoutMilliseconds;
    }

    public void setElectionTimeoutMilliseconds(int electionTimeoutMilliseconds) {
        this.electionTimeoutMilliseconds = electionTimeoutMilliseconds;
    }

    public int getHeartbeatPeriodMilliseconds() {
        return heartbeatPeriodMilliseconds;
    }

    public void setHeartbeatPeriodMilliseconds(int heartbeatPeriodMilliseconds) {
        this.heartbeatPeriodMilliseconds = heartbeatPeriodMilliseconds;
    }

    public int getSnapshotPeriodSeconds() {
        return snapshotPeriodSeconds;
    }

    public void setSnapshotPeriodSeconds(int snapshotPeriodSeconds) {
        this.snapshotPeriodSeconds = snapshotPeriodSeconds;
    }

    public int getSnapshotMinLogSize() {
        return snapshotMinLogSize;
    }

    public void setSnapshotMinLogSize(int snapshotMinLogSize) {
        this.snapshotMinLogSize = snapshotMinLogSize;
    }

    public int getMaxSnapshotBytesPerRequest() {
        return maxSnapshotBytesPerRequest;
    }

    public void setMaxSnapshotBytesPerRequest(int maxSnapshotBytesPerRequest) {
        this.maxSnapshotBytesPerRequest = maxSnapshotBytesPerRequest;
    }

    public int getMaxLogEntriesPerRequest() {
        return maxLogEntriesPerRequest;
    }

    public void setMaxLogEntriesPerRequest(int maxLogEntriesPerRequest) {
        this.maxLogEntriesPerRequest = maxLogEntriesPerRequest;
    }

    public int getMaxSegmentFileSize() {
        return maxSegmentFileSize;
    }

    public void setMaxSegmentFileSize(int maxSegmentFileSize) {
        this.maxSegmentFileSize = maxSegmentFileSize;
    }

    public long getCatchupMargin() {
        return catchupMargin;
    }

    public void setCatchupMargin(long catchupMargin) {
        this.catchupMargin = catchupMargin;
    }

    public long getMaxAwaitTimeout() {
        return maxAwaitTimeout;
    }

    public void setMaxAwaitTimeout(long maxAwaitTimeout) {
        this.maxAwaitTimeout = maxAwaitTimeout;
    }

    public int getRaftConsensusThreadNum() {
        return raftConsensusThreadNum;
    }

    public void setRaftConsensusThreadNum(int raftConsensusThreadNum) {
        this.raftConsensusThreadNum = raftConsensusThreadNum;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

}
