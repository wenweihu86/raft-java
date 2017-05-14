package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.Raft;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public interface RaftClientService {
    Raft.GetLeaderResponse getLeader(Raft.GetLeaderRequest request);
}
