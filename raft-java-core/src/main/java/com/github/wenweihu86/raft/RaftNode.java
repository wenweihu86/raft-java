package com.github.wenweihu86.raft;

import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.storage.SegmentedLog;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.google.protobuf.ByteString;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.github.wenweihu86.rpc.client.RPCCallback;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Created by wenweihu86 on 2017/5/2.
 */
public class RaftNode {

    public enum NodeState {
        STATE_FOLLOWER,
        STATE_CANDIDATE,
        STATE_LEADER
    }

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    private RaftMessage.Configuration configuration;
    private Map<Integer, Peer> peerMap = new HashMap<>();
    private RaftMessage.Server localServer;
    private StateMachine stateMachine;
    private SegmentedLog raftLog;
    private Snapshot snapshot;

    private NodeState state = NodeState.STATE_FOLLOWER;
    // 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private long currentTerm;
    // 在当前获得选票的候选人的Id
    private int votedFor;
    private int leaderId; // leader节点id
    // 已知的最大的已经被提交的日志条目的索引值
    private long commitIndex;
    // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private volatile long lastAppliedIndex;
    private volatile boolean isInSnapshot;

    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = lock.newCondition();
    private Condition catchUpCondition = lock.newCondition();

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    public RaftNode(List<RaftMessage.Server> servers, RaftMessage.Server localServer, StateMachine stateMachine) {
        RaftMessage.Configuration.Builder confBuilder = RaftMessage.Configuration.newBuilder();
        for (RaftMessage.Server server : servers) {
            confBuilder.addServers(server);
        }
        configuration = confBuilder.build();

        this.localServer = localServer;
        this.stateMachine = stateMachine;

        // load log and snapshot
        raftLog = new SegmentedLog();
        snapshot = new Snapshot();

        currentTerm = raftLog.getMetaData().getCurrentTerm();
        votedFor = raftLog.getMetaData().getVotedFor();
        commitIndex = Math.max(snapshot.getMetaData().getLastIncludedIndex(), commitIndex);
        // discard old log entries
        if (snapshot.getMetaData().getLastIncludedIndex() > 0
                && raftLog.getFirstLogIndex() <= snapshot.getMetaData().getLastIncludedIndex()) {
            raftLog.truncatePrefix(snapshot.getMetaData().getLastIncludedIndex() + 1);
        }
        // apply state machine
        RaftMessage.Configuration snapshotConfiguration = snapshot.getMetaData().getConfiguration();
        if (snapshotConfiguration.getServersCount() > 0) {
            configuration = snapshotConfiguration;
        }
        String snapshotDataDir = snapshot.getSnapshotDir() + File.separator + "data";
        stateMachine.readSnapshot(snapshotDataDir);
        for (long index = snapshot.getMetaData().getLastIncludedIndex() + 1;
             index <= commitIndex; index++) {
            RaftMessage.LogEntry entry = raftLog.getEntry(index);
            if (entry.getType() == RaftMessage.EntryType.ENTRY_TYPE_DATA) {
                stateMachine.apply(entry.getData().toByteArray());
            } else if (entry.getType() == RaftMessage.EntryType.ENTRY_TYPE_CONFIGURATION) {
                applyConfiguration(entry);
            }
        }
        lastAppliedIndex = commitIndex;

        for (RaftMessage.Server server : configuration.getServersList()) {
            if (!peerMap.containsKey(server.getServerId())
                    && server.getServerId() != localServer.getServerId()) {
                Peer peer = new Peer(server);
                peer.setNextIndex(raftLog.getLastLogIndex() + 1);
                peerMap.put(server.getServerId(), peer);
            }
        }

        // init thread pool
        executorService = Executors.newFixedThreadPool(peerMap.size() * 2);
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                takeSnapshot();
            }
        }, RaftOptions.snapshotPeriodSeconds, RaftOptions.snapshotPeriodSeconds, TimeUnit.SECONDS);
    }

    public void init() {
        // start election
        resetElectionTimer();
    }

    // client set command
    public boolean replicate(byte[] data, RaftMessage.EntryType entryType) {
        lock.lock();
        long newLastLogIndex = 0;
        try {
            if (state != NodeState.STATE_LEADER) {
                LOG.debug("I'm not the leader");
                return false;
            }
            RaftMessage.LogEntry logEntry = RaftMessage.LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setType(entryType)
                    .setData(ByteString.copyFrom(data)).build();
            List<RaftMessage.LogEntry> entries = new ArrayList<>();
            entries.add(logEntry);
            newLastLogIndex = raftLog.append(entries);
            raftLog.updateMetaData(currentTerm, null, raftLog.getFirstLogIndex());

            for (final Peer peer : peerMap.values()) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        appendEntries(peer);
                    }
                });
            }

            // sync wait commitIndex >= newLastLogIndex
            long startTime = System.currentTimeMillis();
            while (lastAppliedIndex < newLastLogIndex) {
                if (System.currentTimeMillis() - startTime >= RaftOptions.maxAwaitTimeout) {
                    break;
                }
                commitIndexCondition.await(RaftOptions.maxAwaitTimeout, TimeUnit.MILLISECONDS);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            lock.unlock();
        }
        LOG.debug("lastAppliedIndex={} newLastLogIndex={}", lastAppliedIndex, newLastLogIndex);
        if (lastAppliedIndex < newLastLogIndex) {
            return false;
        }
        return true;
    }

    // election timer, request vote
    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                startNewElection();
            }
        }, getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = RaftOptions.electionTimeoutMilliseconds
                + random.nextInt(0, RaftOptions.electionTimeoutMilliseconds);
        LOG.debug("new election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    // 开始新的选举，对candidate有效
    private void startNewElection() {
        lock.lock();
        try {
            if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
                resetElectionTimer();
                return;
            }
            currentTerm++;
            LOG.info("Running for election in term {}", currentTerm);
            state = NodeState.STATE_CANDIDATE;
            leaderId = 0;
            votedFor = localServer.getServerId();
        } finally {
            lock.unlock();
        }

        for (RaftMessage.Server server : configuration.getServersList()) {
            if (server.getServerId() == localServer.getServerId()) {
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    requestVote(peer);
                }
            });
        }
        resetElectionTimer();
    }

    private void requestVote(Peer peer) {
        LOG.info("begin requestVote");
        RaftMessage.VoteRequest.Builder requestBuilder = RaftMessage.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
            requestBuilder.setServerId(localServer.getServerId())
                    .setTerm(currentTerm)
                    .setLastLogIndex(raftLog.getLastLogIndex())
                    .setLastLogTerm(raftLog.getLastLogTerm());
        } finally {
            lock.unlock();
        }

        peer.getRpcClient().asyncCall(
                "RaftConsensusService.requestVote", requestBuilder.build(),
                new VoteResponseCallback(peer));
    }

    private class VoteResponseCallback implements RPCCallback<RaftMessage.VoteResponse> {
        private Peer peer;

        public VoteResponseCallback(Peer peer) {
            this.peer = peer;
        }

        @Override
        public void success(RaftMessage.VoteResponse response) {
            lock.lock();
            try {
                peer.setVoteGranted(response.getGranted());
                if (currentTerm != response.getTerm() || state != NodeState.STATE_CANDIDATE) {
                    LOG.info("ignore requestVote RPC result");
                    return;
                }
                if (response.getTerm() > currentTerm) {
                    LOG.info("Received RequestVote response from server {} " +
                                    "in term {} (this server's term was {})",
                            peer.getServer().getServerId(),
                            response.getTerm(),
                            currentTerm);
                    stepDown(response.getTerm());
                } else {
                    if (response.getGranted()) {
                        LOG.info("Got vote from server {} for term {}",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 0;
                        if (votedFor == localServer.getServerId()) {
                            voteGrantedNum += 1;
                        }
                        for (RaftMessage.Server server : configuration.getServersList()) {
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            Peer peer1 = peerMap.get(server.getServerId());
                            if (peer1.isVoteGranted() != null && peer1.isVoteGranted() == true) {
                                voteGrantedNum += 1;
                            }
                        }
                        LOG.info("voteGrantedNum={}", voteGrantedNum);
                        if (voteGrantedNum > configuration.getServersCount() / 2) {
                            LOG.info("Got majority vote, serverId={} become leader", localServer.getServerId());
                            becomeLeader();
                        }
                    } else {
                        LOG.info("Vote denied by server {} with term {}, my term is {}",
                                peer.getServer().getServerId(), response.getTerm(), currentTerm);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("requestVote with peer[{}:{}] failed",
                    peer.getServer().getEndPoint().getHost(),
                    peer.getServer().getEndPoint().getPort());
            peer.setVoteGranted(new Boolean(false));
        }
    }

    // in lock
    private void becomeLeader() {
        state = NodeState.STATE_LEADER;
        leaderId = localServer.getServerId();
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        // start heartbeat timer
        startNewHeartbeat();
    }

    // heartbeat timer, append entries
    // in lock
    private void resetHeartbeatTimer() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                startNewHeartbeat();
            }
        }, RaftOptions.heartbeatPeriodMilliseconds, TimeUnit.MILLISECONDS);
    }

    // in lock, 开始心跳，对leader有效
    private void startNewHeartbeat() {
        LOG.debug("start new heartbeat");
        for (final Peer peer : peerMap.values()) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    appendEntries(peer);
                }
            });
        }
        resetHeartbeatTimer();
    }

    public void appendEntries(Peer peer) {
        RaftMessage.AppendEntriesRequest.Builder requestBuilder = RaftMessage.AppendEntriesRequest.newBuilder();
        long prevLogIndex;
        long numEntries;
        lock.lock();
        try {
            long firstLogIndex = raftLog.getFirstLogIndex();
            if (peer.getNextIndex() < firstLogIndex) {
                lock.unlock();
                boolean success;
                try {
                    success = installSnapshot(peer);
                } finally {
                    lock.lock();
                }
                if (!success) {
                    return;
                }
            }
            Validate.isTrue(peer.getNextIndex() >= firstLogIndex);

            prevLogIndex = peer.getNextIndex() - 1;
            long prevLogTerm;
            if (prevLogIndex == 0) {
                prevLogTerm = 0;
            } else if (prevLogIndex == snapshot.getMetaData().getLastIncludedIndex()) {
                prevLogTerm = snapshot.getMetaData().getLastIncludedTerm();
            } else {
                prevLogTerm = raftLog.getEntryTerm(prevLogIndex);
            }
            requestBuilder.setServerId(localServer.getServerId());
            requestBuilder.setTerm(currentTerm);
            requestBuilder.setPrevLogTerm(prevLogTerm);
            requestBuilder.setPrevLogIndex(prevLogIndex);
            numEntries = packEntries(peer.getNextIndex(), requestBuilder);
            requestBuilder.setCommitIndex(Math.min(commitIndex, prevLogIndex + numEntries));
        } finally {
            lock.unlock();
        }

        RaftMessage.AppendEntriesRequest request = requestBuilder.build();
        RaftMessage.AppendEntriesResponse response = peer.getRaftConsensusService().appendEntries(request);

        lock.lock();
        try {
            if (response == null) {
                LOG.warn("appendEntries with peer[{}:{}] failed",
                        peer.getServer().getEndPoint().getHost(),
                        peer.getServer().getEndPoint().getPort());
                if (!ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
                    peerMap.remove(peer.getServer().getServerId());
                    peer.getRpcClient().stop();
                }
                return;
            }
            LOG.info("AppendEntries response[{}] from server {} " +
                            "in term {} (my term is {})",
                    response.getResCode(), peer.getServer().getServerId(),
                    response.getTerm(), currentTerm);

            if (response.getTerm() > currentTerm) {
                stepDown(response.getTerm());
            } else {
                if (response.getResCode() == RaftMessage.ResCode.RES_CODE_SUCCESS) {
                    peer.setMatchIndex(prevLogIndex + numEntries);
                    peer.setNextIndex(peer.getMatchIndex() + 1);
                    if (ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
                        advanceCommitIndex();
                    } else {
                        if (raftLog.getLastLogIndex() - peer.getMatchIndex() <= RaftOptions.catchupMargin) {
                            LOG.debug("peer catch up the leader");
                            peer.setCatchUp(true);
                            // signal the caller thread
                            catchUpCondition.signalAll();
                        }
                    }
                } else {
                    peer.setNextIndex(response.getLastLogIndex() + 1);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // in lock, for leader
    private void advanceCommitIndex() {
        // 获取quorum matchIndex
        int peerNum = configuration.getServersList().size();
        long[] matchIndexes = new long[peerNum];
        int i = 0;
        for (RaftMessage.Server server : configuration.getServersList()) {
            if (server.getServerId() != localServer.getServerId()) {
                Peer peer = peerMap.get(server.getServerId());
                matchIndexes[i++] = peer.getMatchIndex();
            }
        }
        matchIndexes[i] = raftLog.getLastLogIndex();
        Arrays.sort(matchIndexes);
        long newCommitIndex = matchIndexes[peerNum / 2];
        LOG.debug("newCommitIndex={}, oldCommitIndex={}", newCommitIndex, commitIndex);
        if (raftLog.getEntryTerm(newCommitIndex) != currentTerm) {
            LOG.debug("newCommitIndexTerm={}, currentTerm={}",
                    raftLog.getEntryTerm(newCommitIndex), currentTerm);
            return;
        }

        if (commitIndex >= newCommitIndex) {
            return;
        }
        long oldCommitIndex = commitIndex;
        commitIndex = newCommitIndex;
        // 同步到状态机
        for (long index = oldCommitIndex + 1; index <= newCommitIndex; index++) {
            RaftMessage.LogEntry entry = raftLog.getEntry(index);
            if (entry.getType() == RaftMessage.EntryType.ENTRY_TYPE_DATA) {
                stateMachine.apply(entry.getData().toByteArray());
            } else if (entry.getType() == RaftMessage.EntryType.ENTRY_TYPE_CONFIGURATION) {
                applyConfiguration(entry);
            }
        }
        lastAppliedIndex = commitIndex;
        LOG.debug("commitIndex={} lastAppliedIndex={}", commitIndex, lastAppliedIndex);
        commitIndexCondition.signalAll();
    }

    // in lock
    private long packEntries(long nextIndex, RaftMessage.AppendEntriesRequest.Builder requestBuilder) {
        long lastIndex = Math.min(raftLog.getLastLogIndex(),
                nextIndex + RaftOptions.maxLogEntriesPerRequest - 1);
        for (long index = nextIndex; index <= lastIndex; index++) {
            RaftMessage.LogEntry entry = raftLog.getEntry(index);
            requestBuilder.addEntries(entry);
        }
        return lastIndex - nextIndex + 1;
    }

    private boolean installSnapshot(Peer peer) {
        LOG.info("begin installSnapshot");
        boolean isSuccess = true;
        boolean isLastRequest = false;
        String lastFileName = null;
        long lastOffset = 0;
        long lastLength = 0;
        while (!isLastRequest) {
            lock.lock();
            RaftMessage.InstallSnapshotRequest request = null;
            try {
                isInSnapshot = true;
                request = buildInstallSnapshotRequest(
                        lastFileName, lastOffset, lastLength);
            } finally {
                lock.unlock();
            }
            if (request == null) {
                isSuccess = false;
                break;
            }
            if (request.getIsLast()) {
                isLastRequest = true;
            }
            RaftMessage.InstallSnapshotResponse response = peer.getRaftConsensusService().installSnapshot(request);
            if (response != null && response.getResCode() == RaftMessage.ResCode.RES_CODE_SUCCESS) {
                lastFileName = request.getFileName();
                lastOffset = request.getOffset();
                lastLength = request.getData().size();
            } else {
                isSuccess = false;
                break;
            }
        }

        lock.lock();
        try {
            if (isSuccess) {
                peer.setNextIndex(snapshot.getMetaData().getLastIncludedIndex() + 1);
            }
            isInSnapshot = false;
        } finally {
            lock.unlock();
        }
        LOG.info("install snapshot success={}", isSuccess);
        return isSuccess;
    }

    // in lock
    private RaftMessage.InstallSnapshotRequest buildInstallSnapshotRequest(
            String lastFileName, long lastOffset, long lastLength) {
        RaftMessage.InstallSnapshotRequest.Builder requestBuilder = RaftMessage.InstallSnapshotRequest.newBuilder();
        try {
            TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap = snapshot.getSnapshotDataFileMap();
            if (lastFileName == null) {
                lastFileName = snapshotDataFileMap.firstKey();
                lastOffset = 0;
                lastLength = 0;
            }
            Snapshot.SnapshotDataFile lastFile = snapshotDataFileMap.get(lastFileName);
            long lastFileLength = lastFile.randomAccessFile.length();
            String currentFileName = lastFileName;
            long currentOffset = lastOffset + lastLength;
            int currentDataSize = RaftOptions.maxSnapshotBytesPerRequest;
            Snapshot.SnapshotDataFile currentDataFile = lastFile;
            if (lastOffset + lastLength < lastFileLength) {
                if (lastOffset + lastLength + RaftOptions.maxSnapshotBytesPerRequest > lastFileLength) {
                    currentDataSize = (int) (lastFileLength - (lastOffset + lastLength));
                }
            } else {
                Map.Entry<String, Snapshot.SnapshotDataFile> currentEntry
                        = snapshotDataFileMap.higherEntry(lastFileName);
                if (currentEntry == null) {
                    return null;
                }
                currentDataFile = currentEntry.getValue();
                currentFileName = currentEntry.getKey();
                currentOffset = 0;
                int currentFileLenght = (int) currentEntry.getValue().randomAccessFile.length();
                if (currentFileLenght < RaftOptions.maxSnapshotBytesPerRequest) {
                    currentDataSize = currentFileLenght;
                }
            }
            byte[] currentData = new byte[currentDataSize];
            currentDataFile.randomAccessFile.read(currentData);
            requestBuilder.setData(ByteString.copyFrom(currentData));
            requestBuilder.setFileName(currentFileName);
            requestBuilder.setOffset(currentOffset);
            requestBuilder.setTerm(currentTerm);
            requestBuilder.setServerId(localServer.getServerId());
            requestBuilder.setIsFirst(false);
            if (currentFileName.equals(snapshotDataFileMap.lastKey())
                    && currentOffset + currentDataSize >= currentDataFile.randomAccessFile.length()) {
                requestBuilder.setIsLast(true);
            } else {
                requestBuilder.setIsLast(false);
            }
            if (currentFileName.equals(snapshotDataFileMap.firstKey()) && currentOffset == 0) {
                requestBuilder.setIsFirst(true);
            } else {
                requestBuilder.setIsFirst(false);
            }
            return requestBuilder.build();
        } catch (IOException ex) {
            return null;
        }
    }

    // in lock
    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            LOG.error("can't be happened");
            return;
        }
        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = 0;
            votedFor = 0;
            raftLog.updateMetaData(currentTerm, votedFor, null);
        }
        state = NodeState.STATE_FOLLOWER;
        // stop heartbeat
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        resetElectionTimer();
    }

    private void takeSnapshot() {
        if (isInSnapshot) {
            return;
        }
        if (lock.tryLock()) {
            try {
                if (raftLog.getTotalSize() < RaftOptions.snapshotMinLogSize) {
                    return;
                }
                if (lastAppliedIndex <= snapshot.getMetaData().getLastIncludedIndex()) {
                    return;
                }
                long lastAppliedTerm = 0;
                if (lastAppliedIndex >= raftLog.getFirstLogIndex()
                        && lastAppliedIndex <= raftLog.getLastLogIndex()) {
                    lastAppliedTerm = raftLog.getEntryTerm(lastAppliedIndex);
                }

                if (!isInSnapshot) {
                    isInSnapshot = true;
                    LOG.info("start taking snapshot");
                    // take snapshot
                    String tmpSnapshotDir = snapshot.getSnapshotDir() + ".tmp";
                    snapshot.updateMetaData(tmpSnapshotDir,
                            lastAppliedIndex, lastAppliedTerm, configuration);
                    String tmpSnapshotDataDir = tmpSnapshotDir + File.separator + "data";
                    stateMachine.writeSnapshot(tmpSnapshotDataDir);
                    // rename tmp snapshot dir to snapshot dir
                    try {
                        File snapshotDirFile = new File(snapshot.getSnapshotDir());
                        if (snapshotDirFile.exists()) {
                            FileUtils.deleteDirectory(snapshotDirFile);
                        }
                        FileUtils.moveDirectory(new File(tmpSnapshotDir),
                                new File(snapshot.getSnapshotDir()));
                    } catch (IOException ex) {
                        LOG.warn("move direct failed when taking snapshot, msg={}", ex.getMessage());
                    }
                    LOG.info("end taking snapshot");
                }
            } finally {
                isInSnapshot = false;
                lock.unlock();
            }
        }
    }

    // in lock
    public void applyConfiguration(RaftMessage.LogEntry entry) {
        try {
            RaftMessage.Configuration newConfiguration
                    = RaftMessage.Configuration.parseFrom(entry.getData().toByteArray());
            configuration = newConfiguration;
            // update peerMap
            for (RaftMessage.Server server : newConfiguration.getServersList()) {
                if (!peerMap.containsKey(server.getServerId())
                        && server.getServerId() != localServer.getServerId()) {
                    Peer peer = new Peer(server);
                    peer.setNextIndex(raftLog.getLastLogIndex() + 1);
                    peerMap.put(server.getServerId(), peer);
                }
            }
            LOG.info("new conf is {}, leaderId={}", PRINTER.print(newConfiguration), leaderId);
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
    }

    public Lock getLock() {
        return lock;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public SegmentedLog getRaftLog() {
        return raftLog;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public RaftMessage.Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(RaftMessage.Configuration configuration) {
        this.configuration = configuration;
    }

    public RaftMessage.Server getLocalServer() {
        return localServer;
    }

    public NodeState getState() {
        return state;
    }

    public Map<Integer, Peer> getPeerMap() {
        return peerMap;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Condition getCatchUpCondition() {
        return catchUpCondition;
    }
}
