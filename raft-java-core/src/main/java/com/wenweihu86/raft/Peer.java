package com.wenweihu86.raft;

import com.wenweihu86.raft.api.RaftApi;
import com.wenweihu86.rpc.client.RPCClient;
import com.wenweihu86.rpc.client.RPCProxy;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {

    private ServerAddress serverAddress;
    private RPCClient rpcClient;
    private RaftApi raftApi;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    private long lastSnapshotIndex;
    private volatile boolean voteGranted;

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

}
