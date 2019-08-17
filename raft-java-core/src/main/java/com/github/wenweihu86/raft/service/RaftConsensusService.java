package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftProto;

/**
 * raft节点之间相互通信的接口。
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusService {

    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

    RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request);

    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);

    RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
}
