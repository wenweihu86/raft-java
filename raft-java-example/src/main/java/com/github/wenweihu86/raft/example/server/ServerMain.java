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

        // 初始化RPCServer
        RPCServer server = new RPCServer(localServer.getEndPoint().getPort());
        // 应用状态机
        ExampleStateMachine stateMachine = new ExampleStateMachine();
        // 设置Raft选项，比如：
        // just for test snapshot
        RaftOptions.snapshotMinLogSize = 10 * 1024;
        RaftOptions.snapshotPeriodSeconds = 30;
        RaftOptions.maxSegmentFileSize = 1024 * 1024;
        // 初始化RaftNode
        RaftNode raftNode = new RaftNode(serverList, localServer, stateMachine);
        // 注册Raft节点之间相互调用的服务
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);
        // 注册给Client调用的Raft服务
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);
        // 注册应用自己提供的服务
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);
        // 启动RPCServer，初始化Raft节点
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
