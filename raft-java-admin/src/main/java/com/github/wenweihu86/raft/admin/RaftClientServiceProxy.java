package com.github.wenweihu86.raft.admin;

import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCClientOptions;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 非线程安全
 * Created by wenweihu86 on 2017/5/14.
 */
public class RaftClientServiceProxy implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceProxy.class);
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    private List<RaftMessage.Server> cluster;
    private RPCClient clusterRPCClient;
    private RaftClientService clusterRaftClientService;

    private RaftMessage.Server leader;
    private RPCClient leaderRPCClient;
    private RaftClientService leaderRaftClientService;

    private RPCClientOptions rpcClientOptions = new RPCClientOptions();

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public RaftClientServiceProxy(String ipPorts) {
        rpcClientOptions.setConnectTimeoutMillis(1000); // 1s
        rpcClientOptions.setReadTimeoutMillis(3600000); // 1hour
        rpcClientOptions.setWriteTimeoutMillis(1000); // 1s
        clusterRPCClient = new RPCClient(ipPorts, rpcClientOptions);
        clusterRaftClientService = RPCProxy.getProxy(clusterRPCClient, RaftClientService.class);
        updateConfiguration();
    }

    @Override
    public RaftMessage.GetLeaderResponse getLeader(RaftMessage.GetLeaderRequest request) {
        return clusterRaftClientService.getLeader(request);
    }

    @Override
    public RaftMessage.GetConfigurationResponse getConfiguration(RaftMessage.GetConfigurationRequest request) {
        return clusterRaftClientService.getConfiguration(request);
    }

    @Override
    public RaftMessage.AddPeersResponse addPeers(RaftMessage.AddPeersRequest request) {
        RaftMessage.AddPeersResponse response = leaderRaftClientService.addPeers(request);
        if (response != null && response.getResCode() == RaftMessage.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.addPeers(request);
        }
        return response;
    }

    @Override
    public RaftMessage.RemovePeersResponse removePeers(RaftMessage.RemovePeersRequest request) {
        RaftMessage.RemovePeersResponse response = leaderRaftClientService.removePeers(request);
        if (response != null && response.getResCode() == RaftMessage.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.removePeers(request);
        }
        return response;
    }

    public void stop() {
        if (leaderRPCClient != null) {
            leaderRPCClient.stop();
        }
        if (clusterRPCClient != null) {
            clusterRPCClient.stop();
        }
    }

    private boolean updateConfiguration() {
        RaftMessage.GetConfigurationRequest request = RaftMessage.GetConfigurationRequest.newBuilder().build();
        RaftMessage.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);
        if (response != null && response.getResCode() == RaftMessage.ResCode.RES_CODE_SUCCESS) {
            if (leaderRPCClient != null) {
                leaderRPCClient.stop();
            }
            leader = response.getLeader();
            leaderRPCClient = new RPCClient(convertEndPoint(leader.getEndPoint()), rpcClientOptions);
            leaderRaftClientService = RPCProxy.getProxy(leaderRPCClient, RaftClientService.class);
            return true;
        }
        return false;
    }

    private EndPoint convertEndPoint(RaftMessage.EndPoint endPoint) {
        return new EndPoint(endPoint.getHost(), endPoint.getPort());
    }

}
