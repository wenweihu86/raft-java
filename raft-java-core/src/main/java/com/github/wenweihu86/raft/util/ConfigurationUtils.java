package com.github.wenweihu86.raft.util;

import com.github.wenweihu86.raft.proto.RaftMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/22.
 */
public class ConfigurationUtils {

    // configuration不会太大，所以这里直接遍历了
    public static boolean containsServer(RaftMessage.Configuration configuration, int serverId) {
        for (RaftMessage.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return true;
            }
        }
        return false;
    }

    public static RaftMessage.Configuration removeServers(
            RaftMessage.Configuration configuration, List<RaftMessage.Server> servers) {
        RaftMessage.Configuration.Builder confBuilder = RaftMessage.Configuration.newBuilder();
        for (RaftMessage.Server server : configuration.getServersList()) {
            boolean toBeRemoved = false;
            for (RaftMessage.Server server1 : servers) {
                if (server.getServerId() == server1.getServerId()) {
                    toBeRemoved = true;
                    break;
                }
            }
            if (!toBeRemoved) {
                confBuilder.addServers(server);
            }
        }
        return confBuilder.build();
    }

    public static RaftMessage.Server getServer(RaftMessage.Configuration configuration, int serverId) {
        for (RaftMessage.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return server;
            }
        }
        return null;
    }

}
