package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftMessage;

/**
 * raft集群管理接口。
 * Created by wenweihu86 on 2017/5/14.
 */
public interface RaftClientService {

    /**
     * 获取raft集群leader节点信息
     * @param request 请求
     * @return leader节点
     */
    RaftMessage.GetLeaderResponse getLeader(RaftMessage.GetLeaderRequest request);

    /**
     * 获取raft集群所有节点信息。
     * @param request 请求
     * @return raft集群各节点地址，以及主从关系。
     */
    RaftMessage.GetConfigurationResponse getConfiguration(RaftMessage.GetConfigurationRequest request);

    /**
     * 向raft集群添加节点。
     * @param request 要添加的节点信息。
     * @return 成功与否。
     */
    RaftMessage.AddPeersResponse addPeers(RaftMessage.AddPeersRequest request);

    /**
     * 从raft集群删除节点
     * @param request 请求
     * @return 成功与否。
     */
    RaftMessage.RemovePeersResponse removePeers(RaftMessage.RemovePeersRequest request);
}
