package com.wenweihu86.raft;

import com.wenweihu86.rpc.client.RPCClient;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer {

    ServerAddress serverAddress;
    RPCClient rpcClient;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;

    public Peer(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        this.rpcClient = new RPCClient(serverAddress.getHost() + ":" + serverAddress.getPort());
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
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

}
