package com.wenweihu86.raft.api.impl;

import com.wenweihu86.raft.RaftNode;
import com.wenweihu86.raft.api.RaftApi;
import com.wenweihu86.raft.proto.Raft;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftApiImpl implements RaftApi {

    private RaftNode node;

    public RaftApiImpl(RaftNode node) {
        this.node = node;
    }

    @Override
    public Raft.VoteResponse requestVote(Raft.VoteRequest request) {
        try {
            node.getLock().lock();
            if (request.getTerm() < node.getCurrentTerm()) {
                Raft.VoteResponse response = Raft.VoteResponse.newBuilder()
                        .setVoteGranted(false)
                        .setTerm(node.getCurrentTerm()).build();
                return response;
            }
            if ((node.getVotedFor() == -1
                    || node.getVotedFor() == request.getCandidateId())
                    && (node.getCurrentTerm() == request.getTerm()
                    && node.getCommitIndex() == request.getLastLogIndex())) {
                Raft.VoteResponse response = Raft.VoteResponse.newBuilder()
                        .setVoteGranted(true)
                        .setTerm(node.getCurrentTerm()).build();
                return response;
            }
        } finally {
            node.getLock().unlock();
        }

        return null;
    }

    @Override
    public Raft.AppendEntriesResponse appendEntries(Raft.AppendEntriesRequest request) {
        return null;
    }

}
