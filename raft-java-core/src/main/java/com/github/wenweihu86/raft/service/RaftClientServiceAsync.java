package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.rpc.client.RPCCallback;

import java.util.concurrent.Future;

/**
 * 用于生成client异步调用所需的proxy
 * Created by wenweihu86 on 2017/5/14.
 */
public interface RaftClientServiceAsync extends RaftClientService {

    Future<RaftMessage.GetLeaderResponse> getLeader(
            RaftMessage.GetLeaderRequest request,
            RPCCallback<RaftMessage.GetLeaderResponse> callback);

    Future<RaftMessage.GetConfigurationResponse> getConfiguration(
            RaftMessage.GetConfigurationRequest request,
            RPCCallback<RaftMessage.GetConfigurationResponse> callback);

    Future<RaftMessage.AddPeersResponse> addPeers(
            RaftMessage.AddPeersRequest request,
            RPCCallback<RaftMessage.AddPeersResponse> callback);

    Future<RaftMessage.RemovePeersResponse> removePeers(
            RaftMessage.RemovePeersRequest request,
            RPCCallback<RaftMessage.RemovePeersResponse> callback);
}
