package com.github.wenweihu86.raft.example;

import com.github.wenweihu86.raft.example.service.ExampleService;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.ServerAddress;
import com.github.wenweihu86.raft.example.service.impl.ExampleServiceImpl;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.service.impl.RaftConsensusServiceImpl;
import com.wenweihu86.rpc.server.RPCServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ServerMain {

    public static void main(String[] args) {
        // parse args
        // serverId
        Integer localServerId = Integer.parseInt(args[0]);
        // format is "host:port:serverId;host2:port2:serverId2"
        String servers = args[1];
        String[] splitArray = servers.split(";");
        List<ServerAddress> serverAddressList = new ArrayList<>();
        for (String serverString : splitArray) {
            String[] splitServer = serverString.split(":");
            String host = splitServer[0];
            Integer port = Integer.parseInt(splitServer[1]);
            Integer serverId = Integer.parseInt(splitServer[2]);
            ServerAddress serverAddress = new ServerAddress(serverId, host, port);
            serverAddressList.add(serverAddress);
        }

        RPCServer server = new RPCServer(8050);

        RaftNode raftNode = new RaftNode(localServerId, serverAddressList);
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);

        ExampleStateMachine stateMachine = new ExampleStateMachine();
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);

        server.start();
    }

}
