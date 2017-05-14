package com.github.wenweihu86.raft.example;

import com.github.wenweihu86.raft.example.service.Example;
import com.github.wenweihu86.raft.example.service.ExampleService;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ExampleServiceProxy implements ExampleService {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceProxy.class);

    private RPCClient clusterRPCClient;
    private ExampleService clusterExampleService;
    private RaftClientService clusterRaftClientService;
    private Raft.EndPoint leader;
    private RPCClient leaderRPCClient;
    private ExampleService leaderExampleService;

    // servers format is 10.1.1.1:8888;10.2.2.2:9999
    public ExampleServiceProxy(RPCClient clusterRPCClient) {
        this.clusterRPCClient = clusterRPCClient;
        clusterExampleService = RPCProxy.getProxy(clusterRPCClient, ExampleService.class);
        clusterRaftClientService = RPCProxy.getProxy(clusterRPCClient, RaftClientService.class);
    }

    @Override
    public Example.SetResponse set(Example.SetRequest request) {
        if (leader == null) {
            getLeader();
            if (leader != null) {
                leaderRPCClient = new RPCClient(leader.getHost() + ":" + leader.getPort());
                leaderExampleService = RPCProxy.getProxy(leaderRPCClient, ExampleService.class);
            }
        }
        if (leader == null || leaderExampleService == null) {
            return null;
        }
        return leaderExampleService.set(request);
    }

    @Override
    public Example.GetResponse get(Example.GetRequest request) {
        return clusterExampleService.get(request);
    }

    private Raft.EndPoint getLeader() {
        if (leader != null) {
            return leader;
        }
        Raft.GetLeaderRequest request = Raft.GetLeaderRequest.newBuilder().build();
        Raft.GetLeaderResponse response = clusterRaftClientService.getLeader(request);
        if (response.getSuccess() == false) {
            LOG.warn("getLeader request failed");
        } else {
            leader = response.getLeader();
        }
        return leader;
    }

}
