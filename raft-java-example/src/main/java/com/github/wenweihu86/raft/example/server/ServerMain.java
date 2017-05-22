package com.github.wenweihu86.raft.example.server;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.example.server.service.impl.ExampleServiceImpl;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.service.impl.RaftClientServiceImpl;
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
        // peers, format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[0];
        String[] splitArray = servers.split(",");
        List<Raft.Server> serverList = new ArrayList<>();
        for (String serverString : splitArray) {
            Raft.Server server = parseServer(serverString);
            serverList.add(server);
        }
        // local server
        Raft.Server localServer = parseServer(args[1]);

        RPCServer server = new RPCServer(localServer.getEndPoint().getPort());

        ExampleStateMachine stateMachine = new ExampleStateMachine();
        // just for test snapshot
        RaftOptions.snapshotMinLogSize = 10 * 1024;
        RaftOptions.snapshotPeriodSeconds = 30;
        RaftOptions.maxSegmentFileSize = 1024 * 1024;
        RaftNode raftNode = new RaftNode(serverList, localServer, stateMachine);

        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);

        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);

        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);

        server.start();
        raftNode.init();
    }

    private static Raft.Server parseServer(String serverString) {
        String[] splitServer = serverString.split(":");
        String host = splitServer[0];
        Integer port = Integer.parseInt(splitServer[1]);
        Integer serverId = Integer.parseInt(splitServer[2]);
        Raft.EndPoint endPoint = Raft.EndPoint.newBuilder()
                .setHost(host).setPort(port).build();
        Raft.Server.Builder serverBuilder = Raft.Server.newBuilder();
        Raft.Server server = serverBuilder.setServerId(serverId).setEndPoint(endPoint).build();
        return server;
    }
}
