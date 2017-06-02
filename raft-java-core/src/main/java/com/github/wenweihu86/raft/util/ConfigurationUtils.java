package com.github.wenweihu86.raft.util;

import com.github.wenweihu86.raft.proto.Raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/22.
 */
public class ConfigurationUtils {

    public static boolean containsServer(Raft.Configuration configuration, int serverId) {
        for (Raft.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return true;
            }
        }
        return false;
    }

    public static Raft.Configuration removeServers(
            Raft.Configuration configuration, List<Raft.Server> servers) {
        Raft.Configuration.Builder confBuilder = Raft.Configuration.newBuilder();
        for (Raft.Server server : configuration.getServersList()) {
            boolean toBeRemoved = false;
            for (Raft.Server server1 : servers) {
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

    public static Raft.Server getServer(Raft.Configuration configuration, int serverId) {
        for (Raft.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return server;
            }
        }
        return null;
    }

}
