package com.github.wenweihu86.raft.service.impl;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftProto;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);
    private static final JsonFormat PRINTER = new JsonFormat();

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk) {
                return responseBuilder.build();
            } else {
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("preVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());
            }
            boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (raftNode.getVotedFor() == 0 && logIsOk) {
                raftNode.stepDown(request.getTerm());
                raftNode.setVotedFor(request.getServerId());
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null, null);
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            LOG.info("RequestVote request from server {} " +
                            "in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.AppendEntriesResponse.Builder responseBuilder
                    = RaftProto.AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
            if (raftNode.getLeaderId() != request.getServerId()) {
                LOG.warn("Another peer={} declares that it is the leader " +
                                "at term={} which was occupied by leader={}",
                        request.getServerId(), request.getTerm(), raftNode.getLeaderId());
                raftNode.stepDown(request.getTerm() + 1);
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
                LOG.info("Rejecting AppendEntries RPC would leave gap, " +
                        "request prevLogIndex={}, my lastLogIndex={}",
                        request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return responseBuilder.build();
            }
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
                    && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex())
                    != request.getPrevLogTerm()) {
                LOG.info("Rejecting AppendEntries RPC: terms don't agree, " +
                        "request prevLogTerm={} in prevLogIndex={}, my is {}",
                        request.getPrevLogTerm(), request.getPrevLogIndex(),
                        raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                Validate.isTrue(request.getPrevLogIndex() > 0);
                responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                return responseBuilder.build();
            }

            if (request.getEntriesCount() == 0) {
                LOG.debug("heartbeat request from peer={} at term={}, my term={}",
                        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
                responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
                advanceCommitIndex(request);
                return responseBuilder.build();
            }

            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex();
            for (RaftProto.LogEntry entry : request.getEntriesList()) {
                index++;
                if (index < raftNode.getRaftLog().getFirstLogIndex()) {
                    continue;
                }
                if (raftNode.getRaftLog().getLastLogIndex() >= index) {
                    if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {
                        continue;
                    }
                    // truncate segment log from index
                    long lastIndexKept = index - 1;
                    raftNode.getRaftLog().truncateSuffix(lastIndexKept);
                }
                entries.add(entry);
            }
            raftNode.getRaftLog().append(entries);
//            raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(),
//                    null, raftNode.getRaftLog().getFirstLogIndex());
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

            advanceCommitIndex(request);
            LOG.info("AppendEntries request from server {} " +
                            "in term {} (my term is {}), entryCount={} resCode={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),
                    request.getEntriesCount(), responseBuilder.getResCode());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder
                = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);

        raftNode.getLock().lock();
        try {
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                LOG.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (raftNode.getSnapshot().getIsTakeSnapshot().get()) {
            LOG.warn("alreay in take snapshot, do not handle install snapshot request now");
            return responseBuilder.build();
        }

        raftNode.getSnapshot().getIsInstallSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        raftNode.getSnapshot().getLock().lock();
        try {
            // write snapshot data to local
            String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst()) {
                if (file.exists()) {
                    file.delete();
                }
                file.mkdir();
                LOG.info("begin accept install snapshot request from serverId={}", request.getServerId());
                raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,
                        request.getSnapshotMetaData().getLastIncludedIndex(),
                        request.getSnapshotMetaData().getLastIncludedTerm(),
                        request.getSnapshotMetaData().getConfiguration());
            }
            // write to file
            String currentDataDirName = tmpSnapshotDir + File.separator + "data";
            File currentDataDir = new File(currentDataDirName);
            if (!currentDataDir.exists()) {
                currentDataDir.mkdirs();
            }

            String currentDataFileName = currentDataDirName + File.separator + request.getFileName();
            File currentDataFile = new File(currentDataFileName);
            // 文件名可能是个相对路径，比如topic/0/message.txt
            if (!currentDataFile.getParentFile().exists()) {
                currentDataFile.getParentFile().mkdirs();
            }
            if (!currentDataFile.exists()) {
                currentDataFile.createNewFile();
            }
            randomAccessFile = RaftFileUtils.openFile(
                    tmpSnapshotDir + File.separator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.seek(request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            // move tmp dir to snapshot dir if this is the last package
            if (request.getIsLast()) {
                File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());
                if (snapshotDirFile.exists()) {
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile);
            }
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            LOG.info("install snapshot request from server {} " +
                            "in term {} (my term is {}), resCode={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getResCode());
        } catch (IOException ex) {
            LOG.warn("when handle installSnapshot request, meet exception:", ex);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
            raftNode.getSnapshot().getLock().unlock();
        }

        if (request.getIsLast() && responseBuilder.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            // apply state machine
            // TODO: make this async
            String snapshotDataDir = raftNode.getSnapshot().getSnapshotDir() + File.separator + "data";
            raftNode.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            // 重新加载snapshot
            raftNode.getSnapshot().getLock().lock();
            try {
                raftNode.getSnapshot().reload();
                lastSnapshotIndex = raftNode.getSnapshot().getMetaData().getLastIncludedIndex();
            } finally {
                raftNode.getSnapshot().getLock().unlock();
            }

            // discard old log entries
            raftNode.getLock().lock();
            try {
                raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            } finally {
                raftNode.getLock().unlock();
            }
            LOG.info("end accept install snapshot request from serverId={}", request.getServerId());
        }

        if (request.getIsLast()) {
            raftNode.getSnapshot().getIsInstallSnapshot().set(false);
        }

        return responseBuilder.build();
    }

    // in lock, for follower
    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
        long newCommitIndex = Math.min(request.getCommitIndex(),
                request.getPrevLogIndex() + request.getEntriesCount());
        raftNode.setCommitIndex(newCommitIndex);
        raftNode.getRaftLog().updateMetaData(null,null, null, newCommitIndex);
        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
            // apply state machine
            for (long index = raftNode.getLastAppliedIndex() + 1;
                 index <= raftNode.getCommitIndex(); index++) {
                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
                if (entry != null) {
                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {
                        raftNode.getStateMachine().apply(entry.getData().toByteArray());
                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        raftNode.applyConfiguration(entry);
                    }
                }
                raftNode.setLastAppliedIndex(index);
            }
        }
    }

}
