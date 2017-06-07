package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftMessage;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public interface RaftClientService {

    RaftMessage.GetLeaderResponse getLeader(RaftMessage.GetLeaderRequest request);

    RaftMessage.GetConfigurationResponse getConfiguration(RaftMessage.GetConfigurationRequest request);

    RaftMessage.AddPeersResponse addPeers(RaftMessage.AddPeersRequest request);

    RaftMessage.RemovePeersResponse removePeers(RaftMessage.RemovePeersRequest request);
}
