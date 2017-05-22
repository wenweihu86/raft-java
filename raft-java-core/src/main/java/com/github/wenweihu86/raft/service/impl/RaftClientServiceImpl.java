package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class RaftClientServiceImpl implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceImpl.class);

    private RaftNode raftNode;

    public RaftClientServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Raft.GetLeaderResponse getLeader(Raft.GetLeaderRequest request) {
        LOG.info("receive getLeader request");
        Raft.GetLeaderResponse.Builder responseBuilder = Raft.GetLeaderResponse.newBuilder();
        responseBuilder.setResCode(Raft.ResCode.RES_CODE_SUCCESS);
        Raft.EndPoint.Builder endPointBuilder = Raft.EndPoint.newBuilder();
        raftNode.getLock().lock();
        try {
            int leaderId = raftNode.getLeaderId();
            if (leaderId == 0) {
                responseBuilder.setResCode(Raft.ResCode.RES_CODE_FAIL);
            } else if (leaderId == raftNode.getLocalServer().getServerId()) {
                endPointBuilder.setHost(raftNode.getLocalServer().getEndPoint().getHost());
                endPointBuilder.setPort(raftNode.getLocalServer().getEndPoint().getPort());
            } else {
                Raft.Configuration configuration = raftNode.getConfiguration();
                for (Raft.Server server : configuration.getServersList()) {
                    if (server.getServerId() == leaderId) {
                        endPointBuilder.setHost(server.getEndPoint().getHost());
                        endPointBuilder.setPort(server.getEndPoint().getPort());
                        break;
                    }
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }
        responseBuilder.setLeader(endPointBuilder.build());
        Raft.GetLeaderResponse response = responseBuilder.build();
        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        try {
            LOG.info("getLeader response={}", printer.print(response));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return responseBuilder.build();
    }

    @Override
    public Raft.AddPeersResponse addPeers(Raft.AddPeersRequest request) {
        Raft.AddPeersResponse.Builder responseBuilder = Raft.AddPeersResponse.newBuilder();
        responseBuilder.setResCode(Raft.ResCode.RES_CODE_FAIL);
        return responseBuilder.build();
    }

    @Override
    public Raft.RemovePeersResponse removePeers(Raft.RemovePeersRequest request) {
        Raft.RemovePeersResponse.Builder responseBuilder = Raft.RemovePeersResponse.newBuilder();
        responseBuilder.setResCode(Raft.ResCode.RES_CODE_FAIL);
        return responseBuilder.build();
    }

}
