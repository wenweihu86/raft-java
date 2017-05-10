package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.Raft;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusService {

    Raft.VoteResponse requestVote(Raft.VoteRequest request);

    Raft.AppendEntriesResponse appendEntries(Raft.AppendEntriesRequest request);

    Raft.InstallSnapshotResponse installSnapshot(Raft.InstallSnapshotRequest request);
}
