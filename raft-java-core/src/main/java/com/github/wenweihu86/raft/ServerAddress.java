package com.github.wenweihu86.raft;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class ServerAddress {

    private int serverId;
    private String host;
    private int port;

    public ServerAddress(int serverId, String host, int port) {
        this.serverId = serverId;
        this.host = host;
        this.port = port;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
