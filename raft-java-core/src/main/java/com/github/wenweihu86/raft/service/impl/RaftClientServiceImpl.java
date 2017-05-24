package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.Peer;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class RaftClientServiceImpl implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceImpl.class);
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

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
        try {
            LOG.info("getLeader response={}", PRINTER.print(response));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return responseBuilder.build();
    }

    @Override
    public Raft.AddPeersResponse addPeers(Raft.AddPeersRequest request) {
        Raft.AddPeersResponse.Builder responseBuilder = Raft.AddPeersResponse.newBuilder();
        responseBuilder.setResCode(Raft.ResCode.RES_CODE_FAIL);
        if (request.getServersCount() == 0
                || request.getServersCount() % 2 != 0) {
            LOG.warn("added server's size can only multiple of 2");
            responseBuilder.setResMsg("added server's size can only multiple of 2");
            return responseBuilder.build();
        }
        for (Raft.Server server : request.getServersList()) {
            if (raftNode.getPeerMap().containsKey(server.getServerId())) {
                LOG.warn("already be added/adding to configuration");
                responseBuilder.setResMsg("already be added/adding to configuration");
                return responseBuilder.build();
            }
        }
        List<Peer> requestPeers = new ArrayList<>(request.getServersCount());
        for (Raft.Server server : request.getServersList()) {
            final Peer peer = new Peer(server);
            peer.setNextIndex(raftNode.getRaftLog().getLastLogIndex() + 1);
            requestPeers.add(peer);

            raftNode.getLock().lock();
            raftNode.getPeerMap().put(server.getServerId(), peer);
            raftNode.getLock().unlock();
            raftNode.getExecutorService().submit(new Runnable() {
                @Override
                public void run() {
                    raftNode.appendEntries(peer);
                }
            });
        }

        int catchUpNum = 0;
        raftNode.getLock().lock();
        try {
            while (catchUpNum < requestPeers.size()) {
                try {
                    raftNode.getCatchUpCondition().await();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                catchUpNum = 0;
                for (Peer peer : requestPeers) {
                    if (peer.isCatchUp()) {
                        catchUpNum++;
                    }
                }
                if (catchUpNum == requestPeers.size()) {
                    break;
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (catchUpNum == requestPeers.size()) {
            raftNode.getLock().lock();
            byte[] configurationData;
            Raft.Configuration newConfiguration;
            try {
                newConfiguration = Raft.Configuration.newBuilder(raftNode.getConfiguration())
                        .addAllServers(request.getServersList()).build();
                configurationData = newConfiguration.toByteArray();
            } finally {
                raftNode.getLock().unlock();
            }
            boolean success = raftNode.replicate(configurationData, Raft.EntryType.ENTRY_TYPE_CONFIGURATION);
            if (success) {
                raftNode.getLock().lock();
                try {
                    raftNode.setConfiguration(newConfiguration);
                } finally {
                    raftNode.getLock().unlock();
                }
                responseBuilder.setResCode(Raft.ResCode.RES_CODE_SUCCESS);
            }
        }
        if (responseBuilder.getResCode() != Raft.ResCode.RES_CODE_SUCCESS) {
            raftNode.getLock().lock();
            try {
                for (Peer peer : requestPeers) {
                    peer.getRpcClient().stop();
                    raftNode.getPeerMap().remove(peer.getServer().getServerId());
                }
            } finally {
                raftNode.getLock().unlock();
            }
        }

        Raft.AddPeersResponse response = responseBuilder.build();
        try {
            LOG.info("addPeers request={} response={}",
                    PRINTER.print(request), PRINTER.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }

        return response;
    }

    @Override
    public Raft.RemovePeersResponse removePeers(Raft.RemovePeersRequest request) {
        Raft.RemovePeersResponse.Builder responseBuilder = Raft.RemovePeersResponse.newBuilder();
        responseBuilder.setResCode(Raft.ResCode.RES_CODE_FAIL);

        if (request.getServersCount() == 0
                || request.getServersCount() % 2 != 0) {
            LOG.warn("removed server's size can only multiple of 2");
            responseBuilder.setResMsg("removed server's size can only multiple of 2");
            return responseBuilder.build();
        }

        // check request peers exist
        raftNode.getLock().lock();
        try {
            for (Raft.Server server : request.getServersList()) {
                if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), server.getServerId())) {
                    return responseBuilder.build();
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }

        raftNode.getLock().lock();
        Raft.Configuration newConfiguration;
        byte[] configurationData;
        try {
            newConfiguration = ConfigurationUtils.removeServers(
                    raftNode.getConfiguration(), request.getServersList());
            configurationData = newConfiguration.toByteArray();
        } finally {
            raftNode.getLock().unlock();
        }
        boolean success = raftNode.replicate(configurationData, Raft.EntryType.ENTRY_TYPE_CONFIGURATION);
        if (success) {
            raftNode.getLock().lock();
            try {
                raftNode.setConfiguration(newConfiguration);
                for (Raft.Server server : request.getServersList()) {
                    Peer peer = raftNode.getPeerMap().remove(server.getServerId());
                    peer.getRpcClient().stop();
                }
            } finally {
                raftNode.getLock().unlock();
            }
            responseBuilder.setResCode(Raft.ResCode.RES_CODE_SUCCESS);
        }

        return responseBuilder.build();
    }

}
