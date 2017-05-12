package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
        raftNode.getLock().lock();
        Raft.VoteResponse.Builder responseBuilder = Raft.VoteResponse.newBuilder();
        responseBuilder.setGranted(false);
        responseBuilder.setTerm(raftNode.getCurrentTerm());
        if (request.getTerm() < raftNode.getCurrentTerm()) {
            raftNode.getLock().unlock();
            return responseBuilder.build();
        }
        if (request.getTerm() > raftNode.getCurrentTerm()) {
            LOG.info("Received RequestVote request from server {} " +
                    "in term {} (this server's term was {})",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm());
            raftNode.stepDown(request.getTerm());
        }
        boolean logIsOk = request.getLastLogTerm() > raftNode.getRaftLog().getLastLogTerm()
                || (request.getLastLogTerm() == raftNode.getRaftLog().getLastLogTerm()
                && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
        if (raftNode.getVotedFor() == 0 || logIsOk) {
            raftNode.stepDown(request.getTerm());
            raftNode.setVotedFor(request.getServerId());
            raftNode.updateMetaData();
            responseBuilder.setGranted(true);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
        }
        raftNode.getLock().unlock();
        return responseBuilder.build();
    }

    @Override
    public Raft.AppendEntriesResponse appendEntries(Raft.AppendEntriesRequest request) {
        raftNode.getLock().lock();
        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();
        responseBuilder.setTerm(raftNode.getCurrentTerm());
        responseBuilder.setSuccess(false);
        responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
        if (request.getTerm() < raftNode.getCurrentTerm()) {
            raftNode.getLock().unlock();
            return responseBuilder.build();
        }
        if (request.getTerm() > raftNode.getCurrentTerm()) {
            LOG.info("Received AppendEntries request from server {} " +
                    "in term {} (this server's term was {})",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm());
            raftNode.stepDown(request.getTerm());
        }
        if (raftNode.getLeaderId() == 0) {
            raftNode.setLeaderId(request.getServerId());
        }

        if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
            LOG.debug("Rejecting AppendEntries RPC: would leave gap");
            raftNode.getLock().unlock();
            return responseBuilder.build();
        }
        if (request.getPrevLogIndex() >= raftNode.getRaftLog().getStartLogIndex()
                && raftNode.getRaftLog().getEntry(request.getPrevLogIndex()).getTerm()
                != request.getPrevLogTerm()) {
            LOG.debug("Rejecting AppendEntries RPC: terms don't agree");
            raftNode.getLock().unlock();
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
                // truncate segment log from index
                long lastIndexKept = index - 1;
                raftNode.getRaftLog().truncateSuffix(lastIndexKept);
            }
            entries.add(entry);
        }
        raftNode.getRaftLog().append(entries);
        responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

        if (raftNode.getCommitIndex() < request.getCommitIndex()) {
            raftNode.setCommitIndex(request.getCommitIndex());
            // apply state machine
            for (index = raftNode.getLastAppliedIndex() + 1; index <= raftNode.getCommitIndex(); index++) {
                raftNode.getStateMachine().apply(
                        raftNode.getRaftLog().getEntry(index).getData().toByteArray());
            }
        }

        raftNode.getLock().unlock();
        return responseBuilder.build();
    }

    @Override
    public Raft.InstallSnapshotResponse installSnapshot(Raft.InstallSnapshotRequest request) {
        Raft.InstallSnapshotResponse.Builder responseBuilder = Raft.InstallSnapshotResponse.newBuilder();
        responseBuilder.setTerm(raftNode.getCurrentTerm());

        raftNode.getLock().lock();
        if (request.getTerm() < raftNode.getCurrentTerm()) {
            LOG.info("Caller({}) is stale. Our term is {}, theirs is {}",
                    request.getServerId(), raftNode.getCurrentTerm(), request.getTerm());
            raftNode.getLock().unlock();
            return responseBuilder.build();
        }
        raftNode.stepDown(request.getTerm());
        if (raftNode.getLeaderId() == 0) {
            raftNode.setLeaderId(request.getServerId());
        }
        raftNode.getLock().unlock();

        // write snapshot data to local
        raftNode.getSnapshotLock().writeLock().lock();
        String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";
        File file = new File(tmpSnapshotDir);
        if (file.exists() && request.getIsFirst()) {
            file.delete();
            file.mkdir();
        }
        if (request.getIsFirst()) {
            raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,
                    request.getSnapshotMetaData().getLastIncludedIndex(),
                    request.getSnapshotMetaData().getLastIncludedTerm());
        }
        // write to file
        RandomAccessFile randomAccessFile = null;
        try {
            String currentDataFileName = tmpSnapshotDir + File.pathSeparator
                    + "data" + File.pathSeparator + request.getFileName();
            File currentDataFile = new File(currentDataFileName);
            if (!currentDataFile.exists()) {
                currentDataFile.createNewFile();
            }
            randomAccessFile = RaftFileUtils.openFile(
                    tmpSnapshotDir + File.pathSeparator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.skipBytes((int) request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                    randomAccessFile = null;
                } catch (Exception ex2) {
                    LOG.warn("close failed");
                }
            }
            // move tmp dir to snapshot dir if this is the last package
            File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());
            if (snapshotDirFile.exists()) {
                snapshotDirFile.delete();
            }
            FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile);
            responseBuilder.setSuccess(true);
        } catch (IOException ex) {
            LOG.warn("io exception, msg={}", ex.getMessage());
        } finally {
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (Exception ex2) {
                    LOG.warn("close failed");
                }
            }
            raftNode.getSnapshotLock().writeLock().unlock();
        }
        return responseBuilder.build();
    }

}
