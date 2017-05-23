package com.github.wenweihu86.raft;

import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {
    private Raft.Server server;
    private RPCClient rpcClient;
    private RaftConsensusService raftConsensusService;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;

    public Peer(Raft.Server server) {
        this.server = server;
        this.rpcClient = new RPCClient(new EndPoint(
                server.getEndPoint().getHost(),
                server.getEndPoint().getPort()));
        raftConsensusService = RPCProxy.getProxy(rpcClient, RaftConsensusService.class);
        isCatchUp = false;
    }

    public Raft.Server getServer() {
        return server;
    }

    public RPCClient getRpcClient() {
        return rpcClient;
    }

    public RaftConsensusService getRaftConsensusService() {
        return raftConsensusService;
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

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
}
