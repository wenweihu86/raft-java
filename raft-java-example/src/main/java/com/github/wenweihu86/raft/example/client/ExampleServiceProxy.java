package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.Example;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 非线程安全
 * Created by wenweihu86 on 2017/5/14.
 */
public class ExampleServiceProxy implements ExampleService {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceProxy.class);
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    protected List<Raft.Server> cluster;
    protected RPCClient clusterRPCClient;
    protected RaftClientService clusterRaftClientService;
    private ExampleService clusterExampleService;

    protected Raft.Server leader;
    protected RPCClient leaderRPCClient;
    protected RaftClientService leaderRaftClientService;
    private ExampleService leaderExampleService;

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public ExampleServiceProxy(String ipPorts) {
        clusterRPCClient = new RPCClient(ipPorts);
        clusterExampleService = RPCProxy.getProxy(clusterRPCClient, ExampleService.class);
        clusterRaftClientService = RPCProxy.getProxy(clusterRPCClient, RaftClientService.class);
        updateConfiguration();
    }

    @Override
    public Example.SetResponse set(Example.SetRequest request) {
        int maxTryCount = 3;
        int currentTryCount = 0;
        Example.SetResponse response = leaderExampleService.set(request);
        while (response != null && !response.getSuccess() && currentTryCount++ < maxTryCount) {
            updateConfiguration();
            response = leaderExampleService.set(request);
        }
        try {
            LOG.info("set request={} response={}",
                    PRINTER.print(request), PRINTER.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
        return response;
    }

    @Override
    public Example.GetResponse get(Example.GetRequest request) {
        Example.GetResponse response = Example.GetResponse.newBuilder().build();
        try {
            response = clusterExampleService.get(request);
            LOG.info("get request={} response={}",
                    PRINTER.print(request), PRINTER.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
        return response;
    }

    private boolean updateConfiguration() {
        Raft.GetConfigurationRequest request = Raft.GetConfigurationRequest.newBuilder().build();
        Raft.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);
        if (response != null && response.getResCode() == Raft.ResCode.RES_CODE_SUCCESS) {
            leaderRPCClient.stop();
            leader = response.getLeader();
            leaderRPCClient = new RPCClient(convertEndPoint(leader.getEndPoint()));
            leaderRaftClientService = RPCProxy.getProxy(leaderRPCClient, RaftClientService.class);
            leaderExampleService = RPCProxy.getProxy(leaderRPCClient, ExampleService.class);
            return true;
        }
        return false;
    }

    private EndPoint convertEndPoint(Raft.EndPoint endPoint) {
        return new EndPoint(endPoint.getHost(), endPoint.getPort());
    }

    private List<EndPoint> convertEndPoints(List<Raft.Server> servers) {
        List<EndPoint> endPoints = new ArrayList<>(servers.size());
        for (Raft.Server server : servers) {
            endPoints.add(convertEndPoint(server.getEndPoint()));
        }
        return endPoints;
    }

}
