package com.github.wenweihu86.raft.admin;

import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
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

    protected List<Raft.Server> cluster;
    protected RPCClient clusterRPCClient;
    protected RaftClientService clusterRaftClientService;

    protected Raft.Server leader;
    protected RPCClient leaderRPCClient;
    protected RaftClientService leaderRaftClientService;

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public RaftClientServiceProxy(String ipPorts) {
        clusterRPCClient = new RPCClient(ipPorts);
        clusterRaftClientService = RPCProxy.getProxy(clusterRPCClient, RaftClientService.class);
        updateConfiguration();
    }

    @Override
    public Raft.GetLeaderResponse getLeader(Raft.GetLeaderRequest request) {
        return clusterRaftClientService.getLeader(request);
    }

    @Override
    public Raft.GetConfigurationResponse getConfiguration(Raft.GetConfigurationRequest request) {
        return clusterRaftClientService.getConfiguration(request);
    }

    @Override
    public Raft.AddPeersResponse addPeers(Raft.AddPeersRequest request) {
        Raft.AddPeersResponse response = leaderRaftClientService.addPeers(request);
        if (response != null && response.getResCode() == Raft.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
        }
        return leaderRaftClientService.addPeers(request);
    }

    @Override
    public Raft.RemovePeersResponse removePeers(Raft.RemovePeersRequest request) {
        Raft.RemovePeersResponse response = leaderRaftClientService.removePeers(request);
        if (response != null && response.getResCode() == Raft.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
        }
        return leaderRaftClientService.removePeers(request);
    }

    private boolean updateConfiguration() {
        Raft.GetConfigurationRequest request = Raft.GetConfigurationRequest.newBuilder().build();
        Raft.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);
        if (response != null && response.getResCode() == Raft.ResCode.RES_CODE_SUCCESS) {
            leaderRPCClient.stop();
            leader = response.getLeader();
            leaderRPCClient = new RPCClient(convertEndPoint(leader.getEndPoint()));
            leaderRaftClientService = RPCProxy.getProxy(leaderRPCClient, RaftClientService.class);
            return true;
        }
        return false;
    }

    private EndPoint convertEndPoint(Raft.EndPoint endPoint) {
        return new EndPoint(endPoint.getHost(), endPoint.getPort());
    }

}
