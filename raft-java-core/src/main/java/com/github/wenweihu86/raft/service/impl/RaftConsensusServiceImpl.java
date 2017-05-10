package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);
    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
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
                raftNode.stepDown(raftNode.getCurrentTerm());
                raftNode.resetElectionTimer();
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
        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();
        responseBuilder.setTerm(raftNode.getCurrentTerm());
        responseBuilder.setSuccess(false);
        responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

        if (request.getTerm() < raftNode.getCurrentTerm()) {
            return responseBuilder.build();
        }
        if (request.getTerm() > raftNode.getCurrentTerm()) {
            LOG.info("Received AppendEntries request from server {} " +
                    "in term {} (this server's term was {})",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm());
            responseBuilder.setTerm(request.getTerm());
        }
        raftNode.stepDown(request.getTerm());
        raftNode.resetElectionTimer();
        if (raftNode.getLeaderId() == 0) {
            raftNode.setLeaderId(request.getServerId());
        }
        if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
            LOG.debug("Rejecting AppendEntries RPC: would leave gap");
            return responseBuilder.build();
        }
        if (request.getPrevLogIndex() >= raftNode.getRaftLog().getStartLogIndex()
                && raftNode.getRaftLog().getEntry(request.getPrevLogIndex()).getTerm()
                != request.getPrevLogTerm()) {
            LOG.debug("Rejecting AppendEntries RPC: terms don't agree");
            return responseBuilder.build();
        }

        responseBuilder.setSuccess(true);
        List<Raft.LogEntry> entries = new ArrayList<>();
        long index = request.getPrevLogIndex();
        for (Raft.LogEntry entry : request.getEntriesList()) {
            index++;
            if (index < raftNode.getRaftLog().getStartLogIndex()) {
                continue;
            }
            if (raftNode.getRaftLog().getLastLogIndex() >= index) {
                if (raftNode.getRaftLog().getEntry(index).getTerm() == entry.getTerm()) {
                    continue;
                }
                // TODO: truncate segment log from index
            }
            entries.add(entry);
        }
        raftNode.getRaftLog().append(entries);
        responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

        if (raftNode.getCommitIndex() < request.getCommitIndex()) {
            raftNode.setCommitIndex(request.getCommitIndex());
            // TODO: apply state machine
        }

        return responseBuilder.build();
    }

    @Override
    public Raft.InstallSnapshotResponse installSnapshot(Raft.InstallSnapshotRequest request) {
        return null;
    }

}
