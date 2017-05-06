package com.wenweihu86.raft.api.impl;

import com.wenweihu86.raft.RaftNode;
import com.wenweihu86.raft.api.RaftApi;
import com.wenweihu86.raft.proto.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftApiImpl implements RaftApi {

    private static final Logger LOG = LoggerFactory.getLogger(RaftApiImpl.class);
    private RaftNode raftNode;

    public RaftApiImpl(RaftNode node) {
        this.raftNode = node;
    }

    @Override
    public Raft.VoteResponse requestVote(Raft.VoteRequest request) {
        if (request.getTerm() > raftNode.getCurrentTerm()) {
            LOG.info("Received RequestVote request from server {} " +
                    "in term {} (this server's term was {})",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm());
            raftNode.stepDown(request.getTerm());
        }
        if (request.getTerm() == raftNode.getCurrentTerm()) {
            if ((raftNode.getVotedFor() == 0
                    || raftNode.getVotedFor() == request.getServerId())
                    && (raftNode.getCurrentTerm() == request.getTerm()
                    && raftNode.getCommitIndex() == request.getLastLogIndex())) {
                raftNode.setVotedFor(request.getServerId());
                raftNode.updateMetaData();
                Raft.VoteResponse response = Raft.VoteResponse.newBuilder()
                        .setGranted(true)
                        .setTerm(raftNode.getCurrentTerm()).build();
                return response;
            }
        }

        Raft.VoteResponse response = Raft.VoteResponse.newBuilder()
                .setGranted(false)
                .setTerm(raftNode.getCurrentTerm()).build();
        return response;
    }

    @Override
    public Raft.AppendEntriesResponse appendEntries(Raft.AppendEntriesRequest request) {
        return null;
    }

}
