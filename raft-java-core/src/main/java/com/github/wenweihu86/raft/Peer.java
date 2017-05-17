package com.github.wenweihu86.raft;

import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {
    private ServerAddress serverAddress;
    private RPCClient rpcClient;
    private RaftConsensusService raftConsensusService;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    private volatile Boolean voteGranted;

    public Peer(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        this.rpcClient = new RPCClient(new EndPoint(serverAddress.getHost(), serverAddress.getPort()));
        raftConsensusService = RPCProxy.getProxy(rpcClient, RaftConsensusService.class);
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
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

}
