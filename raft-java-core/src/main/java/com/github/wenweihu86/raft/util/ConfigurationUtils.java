package com.github.wenweihu86.raft.util;

import com.github.wenweihu86.raft.proto.Raft;

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

}
