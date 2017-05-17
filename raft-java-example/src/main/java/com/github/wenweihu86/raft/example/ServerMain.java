package com.github.wenweihu86.raft.example;

import com.github.wenweihu86.raft.example.service.ExampleService;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.ServerAddress;
import com.github.wenweihu86.raft.example.service.impl.ExampleServiceImpl;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.service.impl.RaftConsensusServiceImpl;
import com.github.wenweihu86.rpc.server.RPCServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ServerMain {
    public static void main(String[] args) {
        // parse args
        // serverId
        Integer localServerId = Integer.parseInt(args[1]);
        // format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[0];
        String[] splitArray = servers.split(",");
        List<ServerAddress> serverAddressList = new ArrayList<>();
        ServerAddress localServer = null;
        for (String serverString : splitArray) {
            String[] splitServer = serverString.split(":");
            String host = splitServer[0];
            Integer port = Integer.parseInt(splitServer[1]);
            Integer serverId = Integer.parseInt(splitServer[2]);
            ServerAddress serverAddress = new ServerAddress(serverId, host, port);
            serverAddressList.add(serverAddress);
            if (serverId.equals(localServerId)) {
                localServer = new ServerAddress(serverId, host, port);
            }
        }

        RPCServer server = new RPCServer(localServer.getPort());

        ExampleStateMachine stateMachine = new ExampleStateMachine();
        RaftNode raftNode = new RaftNode(localServerId, serverAddressList, stateMachine);
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);

        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);

        server.start();
        raftNode.init();
    }
}
