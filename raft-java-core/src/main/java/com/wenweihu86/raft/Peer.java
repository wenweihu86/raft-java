package com.wenweihu86.raft;

import com.wenweihu86.raft.api.RaftApi;
import com.wenweihu86.rpc.client.RPCClient;
import com.wenweihu86.rpc.client.RPCProxy;

import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {

    private RaftNode raftNode;
    private ServerAddress serverAddress;
    private RPCClient rpcClient;
    private RaftApi raftApi;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    private String lastSnapshotFileName;
    private long lastSnapshotFileOffset;
    private long lastSnapshotIndex;
    private volatile boolean voteGranted;
    private Future electionFuture;
    private Future heartbeatFuture;

    public Peer(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        this.rpcClient = new RPCClient(serverAddress.getHost() + ":" + serverAddress.getPort());
        raftApi = RPCProxy.getProxy(rpcClient, RaftApi.class);
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    public RPCClient getRpcClient() {
        return rpcClient;
    }

    public void setRpcClient(RPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    public RaftApi getRaftApi() {
        return raftApi;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public String getLastSnapshotFileName() {
        return lastSnapshotFileName;
    }

    public void setLastSnapshotFileName(String lastSnapshotFileName) {
        this.lastSnapshotFileName = lastSnapshotFileName;
    }

    public long getLastSnapshotFileOffset() {
        return lastSnapshotFileOffset;
    }

    public void setLastSnapshotFileOffset(long lastSnapshotFileOffset) {
        this.lastSnapshotFileOffset = lastSnapshotFileOffset;
    }

    public long getLastSnapshotIndex() {
        return lastSnapshotIndex;
    }

    public void setLastSnapshotIndex(long lastSnapshotIndex) {
        this.lastSnapshotIndex = lastSnapshotIndex;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    public Future getElectionFuture() {
        return electionFuture;
    }

    public void setElectionFuture(Future electionFuture) {
        this.electionFuture = electionFuture;
    }

    public Future getHeartbeatFuture() {
        return heartbeatFuture;
    }

    public void setHeartbeatFuture(Future heartbeatFuture) {
        this.heartbeatFuture = heartbeatFuture;
    }
}
