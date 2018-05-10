package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftMessage;

/**
 * raft节点之间相互通信的接口。
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusService {

    RaftMessage.VoteResponse preVote(RaftMessage.VoteRequest request);

    RaftMessage.VoteResponse requestVote(RaftMessage.VoteRequest request);

    RaftMessage.AppendEntriesResponse appendEntries(RaftMessage.AppendEntriesRequest request);

    RaftMessage.InstallSnapshotResponse installSnapshot(RaftMessage.InstallSnapshotRequest request);
}
