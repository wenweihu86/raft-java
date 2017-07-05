package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.Peer;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftMessage;
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
    public RaftMessage.GetLeaderResponse getLeader(RaftMessage.GetLeaderRequest request) {
        LOG.info("receive getLeader request");
        RaftMessage.GetLeaderResponse.Builder responseBuilder = RaftMessage.GetLeaderResponse.newBuilder();
        responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_SUCCESS);
        RaftMessage.EndPoint.Builder endPointBuilder = RaftMessage.EndPoint.newBuilder();
        raftNode.getLock().lock();
        try {
            int leaderId = raftNode.getLeaderId();
            if (leaderId == 0) {
                responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_FAIL);
            } else if (leaderId == raftNode.getLocalServer().getServerId()) {
                endPointBuilder.setHost(raftNode.getLocalServer().getEndPoint().getHost());
                endPointBuilder.setPort(raftNode.getLocalServer().getEndPoint().getPort());
            } else {
                RaftMessage.Configuration configuration = raftNode.getConfiguration();
                for (RaftMessage.Server server : configuration.getServersList()) {
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
        RaftMessage.GetLeaderResponse response = responseBuilder.build();
        try {
            LOG.info("getLeader response={}", PRINTER.print(response));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return responseBuilder.build();
    }

    @Override
    public RaftMessage.GetConfigurationResponse getConfiguration(RaftMessage.GetConfigurationRequest request) {
        RaftMessage.GetConfigurationResponse.Builder responseBuilder
                = RaftMessage.GetConfigurationResponse.newBuilder();
        responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_SUCCESS);
        raftNode.getLock().lock();
        try {
            RaftMessage.Configuration configuration = raftNode.getConfiguration();
            RaftMessage.Server leader = ConfigurationUtils.getServer(configuration, raftNode.getLeaderId());
            responseBuilder.setLeader(leader);
            responseBuilder.addAllServers(configuration.getServersList());
        } finally {
            raftNode.getLock().unlock();
        }
        RaftMessage.GetConfigurationResponse response = responseBuilder.build();
        try {
            LOG.info("getConfiguration request={} response={}",
                    PRINTER.print(request), PRINTER.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }

        return response;
    }

    @Override
    public RaftMessage.AddPeersResponse addPeers(RaftMessage.AddPeersRequest request) {
        RaftMessage.AddPeersResponse.Builder responseBuilder = RaftMessage.AddPeersResponse.newBuilder();
        responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_FAIL);
        if (request.getServersCount() == 0
                || request.getServersCount() % 2 != 0) {
            LOG.warn("added server's size can only multiple of 2");
            responseBuilder.setResMsg("added server's size can only multiple of 2");
            return responseBuilder.build();
        }
        for (RaftMessage.Server server : request.getServersList()) {
            if (raftNode.getPeerMap().containsKey(server.getServerId())) {
                LOG.warn("already be added/adding to configuration");
                responseBuilder.setResMsg("already be added/adding to configuration");
                return responseBuilder.build();
            }
        }
        List<Peer> requestPeers = new ArrayList<>(request.getServersCount());
        for (RaftMessage.Server server : request.getServersList()) {
            final Peer peer = new Peer(server);
            peer.setNextIndex(1);
            requestPeers.add(peer);
            raftNode.getPeerMap().putIfAbsent(server.getServerId(), peer);
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
            RaftMessage.Configuration newConfiguration;
            try {
                newConfiguration = RaftMessage.Configuration.newBuilder(raftNode.getConfiguration())
                        .addAllServers(request.getServersList()).build();
                configurationData = newConfiguration.toByteArray();
            } finally {
                raftNode.getLock().unlock();
            }
            boolean success = raftNode.replicate(configurationData, RaftMessage.EntryType.ENTRY_TYPE_CONFIGURATION);
            if (success) {
                responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_SUCCESS);
            }
        }
        if (responseBuilder.getResCode() != RaftMessage.ResCode.RES_CODE_SUCCESS) {
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

        RaftMessage.AddPeersResponse response = responseBuilder.build();
        try {
            LOG.info("addPeers request={} resCode={}",
                    PRINTER.print(request), response.getResCode());
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }

        return response;
    }

    @Override
    public RaftMessage.RemovePeersResponse removePeers(RaftMessage.RemovePeersRequest request) {
        RaftMessage.RemovePeersResponse.Builder responseBuilder = RaftMessage.RemovePeersResponse.newBuilder();
        responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_FAIL);

        if (request.getServersCount() == 0
                || request.getServersCount() % 2 != 0) {
            LOG.warn("removed server's size can only multiple of 2");
            responseBuilder.setResMsg("removed server's size can only multiple of 2");
            return responseBuilder.build();
        }

        // check request peers exist
        raftNode.getLock().lock();
        try {
            for (RaftMessage.Server server : request.getServersList()) {
                if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), server.getServerId())) {
                    return responseBuilder.build();
                }
            }
        } finally {
            raftNode.getLock().unlock();
        }

        raftNode.getLock().lock();
        RaftMessage.Configuration newConfiguration;
        byte[] configurationData;
        try {
            newConfiguration = ConfigurationUtils.removeServers(
                    raftNode.getConfiguration(), request.getServersList());
            try {
                LOG.debug("newConfiguration={}", PRINTER.print(newConfiguration));
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }
            configurationData = newConfiguration.toByteArray();
        } finally {
            raftNode.getLock().unlock();
        }
        boolean success = raftNode.replicate(configurationData, RaftMessage.EntryType.ENTRY_TYPE_CONFIGURATION);
        if (success) {
            responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_SUCCESS);
        }

        try {
            LOG.info("removePeers request={} resCode={}",
                    PRINTER.print(request), responseBuilder.getResCode());
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }

        return responseBuilder.build();
    }

}
