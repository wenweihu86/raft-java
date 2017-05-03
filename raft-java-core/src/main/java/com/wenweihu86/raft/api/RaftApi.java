package com.wenweihu86.raft.api;

import com.wenweihu86.raft.proto.Raft;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftApi {

    Raft.VoteResponse requestVote(Raft.VoteRequest request);

    Raft.AppendEntriesResponse appendEntries(Raft.AppendEntriesRequest request);
}
