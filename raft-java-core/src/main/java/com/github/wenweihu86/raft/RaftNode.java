package com.github.wenweihu86.raft;

import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.storage.SegmentedLog;
import com.google.protobuf.ByteString;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.github.wenweihu86.rpc.client.RPCCallback;
import org.apache.commons.io.FileUtils;
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

    private NodeState state = NodeState.STATE_FOLLOWER;
    // 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private long currentTerm;
    // 在当前获得选票的候选人的Id
    private int votedFor;
    private List<Raft.LogEntry> entries;
    // 已知的最大的已经被提交的日志条目的索引值
    private long commitIndex;
    // The index of the last log entry that has been flushed to disk.
    // Valid for leaders only.
    private long lastSyncedIndex;
    // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private volatile long lastAppliedIndex;

    private List<Peer> peers;
    private ServerAddress localServer;
    private int leaderId; // leader节点id
    private SegmentedLog raftLog;
    private StateMachine stateMachine;

    private Snapshot snapshot;
    private long lastSnapshotIndex;
    private long lastSnapshotTerm;
    private boolean isSnapshoting;
    private ReadWriteLock snapshotLock = new ReentrantReadWriteLock();

    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = lock.newCondition();

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    public RaftNode(int localServerId, List<ServerAddress> servers) {
        raftLog = new SegmentedLog();
        snapshot = new Snapshot();
        for (ServerAddress server : servers) {
            if (server.getServerId() == localServerId) {
                this.localServer = server;
            } else {
                Peer peer = new Peer(server);
                peers.add(peer);
            }
        }
        executorService = Executors.newFixedThreadPool(peers.size() * 2);
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        // election timer
        resetElectionTimer();
        this.currentTerm = raftLog.getMetaData().getCurrentTerm();
        this.votedFor = raftLog.getMetaData().getVotedFor();
        this.commitIndex = Math.max(snapshot.getMetaData().getLastIncludedIndex(), commitIndex);
        stepDown(1);
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                takeSnapshot();
            }
        }, RaftOption.snapshotPeriodSeconds, TimeUnit.SECONDS);
    }

    public void resetElectionTimer() {
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

    // 开始新的选举，对candidate有效
    public void startNewElection() {
        currentTerm++;
        LOG.info("Running for election in term {}", currentTerm);
        state = NodeState.STATE_CANDIDATE;
        leaderId = 0;
        votedFor = localServer.getServerId();
        for (final Peer peer : peers) {
            Future electionFuture = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    requestVote(peer);
                }
            });
            peer.setElectionFuture(electionFuture);
        }
        resetElectionTimer();
    }

    public void resetHeartbeatTimer() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                startNewHeartbeat();
            }
        }, RaftOption.heartbeatPeriodMilliseconds, TimeUnit.MILLISECONDS);
    }

    // 开始心跳，对leader有效
    public void startNewHeartbeat() {
        LOG.info("start new heartbeat");
        for (final Peer peer : peers) {
            Future heartbeatFuture = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    appendEntries(peer);
                }
            });
            peer.setHeartbeatFuture(heartbeatFuture);
        }
        resetHeartbeatTimer();
    }

    public void requestVote(Peer peer) {
        peer.setVoteGranted(false);
        Raft.VoteRequest request = Raft.VoteRequest.newBuilder()
                .setServerId(localServer.getServerId())
                .setTerm(currentTerm)
                .setLastLogIndex(raftLog.getLastLogIndex())
                .setLastLogTerm(raftLog.getLastLogTerm()).build();
        peer.getRpcClient().asyncCall(
                "RaftConsensusService.requestVote", request,
                new VoteResponseCallback(peer));
    }

    private class VoteResponseCallback implements RPCCallback<Raft.VoteResponse> {
        private Peer peer;

        public VoteResponseCallback(Peer peer) {
            this.peer = peer;
        }

        @Override
        public void success(Raft.VoteResponse response) {
            if (response.getTerm() > currentTerm) {
                LOG.info("Received RequestVote response from server {} " +
                                "in term {} (this server's term was {})",
                        peer.getServerAddress().getServerId(),
                        response.getTerm(),
                        currentTerm);
                stepDown(response.getTerm());
            } else {
                if (response.getGranted()) {
                    LOG.info("Got vote from server {} for term {}",
                            peer.getServerAddress().getServerId(), currentTerm);
                    int voteGrantedNum = 1;
                    for (Peer peer1 : peers) {
                        if (peer1.isVoteGranted()) {
                            voteGrantedNum += 1;
                        }
                    }
                    if (voteGrantedNum > (peers.size() + 1) / 2) {
                        LOG.info("Got majority vote");
                        becomeLeader();
                    }
                } else {
                    LOG.info("Vote denied by server {} for term {}",
                            peer.getServerAddress().getServerId(), currentTerm);
                }
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("requestVote with peer[{}:{}] failed",
                    peer.getServerAddress().getHost(),
                    peer.getServerAddress().getPort());
        }

    }

    public void appendEntries(Peer peer) {
        long startLogIndex = raftLog.getStartLogIndex();
        if (peer.getNextIndex() < startLogIndex) {
            this.installSnapshot(peer);
            return;
        }

        long lastLogIndex = this.raftLog.getLastLogIndex();
        long prevLogIndex = peer.getNextIndex() - 1;
        long prevLogTerm = 0;
        if (prevLogIndex >= startLogIndex) {
            prevLogTerm = raftLog.getEntry(prevLogIndex).getTerm();
        } else if (prevLogIndex == 0) {
            prevLogTerm = 0;
        } else if (prevLogIndex == lastSnapshotIndex) {
            prevLogTerm = lastSnapshotTerm;
        } else {
            installSnapshot(peer);
            return;
        }

        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();
        requestBuilder.setServerId(localServer.getServerId());
        requestBuilder.setTerm(currentTerm);
        requestBuilder.setPrevLogTerm(prevLogTerm);
        requestBuilder.setPrevLogIndex(prevLogIndex);
        long numEntries = packEntries(peer.getNextIndex(), requestBuilder);
        requestBuilder.setCommitIndex(Math.min(commitIndex, prevLogIndex + numEntries));
        Raft.AppendEntriesRequest request = requestBuilder.build();

        Raft.AppendEntriesResponse response = peer.getRaftConsensusService().appendEntries(request);
        if (response == null) {
            LOG.warn("appendEntries with peer[{}:{}] failed",
                    peer.getServerAddress().getHost(),
                    peer.getServerAddress().getPort());
            return;
        }
        if (response.getTerm() > currentTerm) {
            LOG.info("Received AppendEntries response from server {} " +
                    "in term {} (this server's term was {})",
                    peer.getServerAddress().getServerId(),
                    response.getTerm(), currentTerm);
            stepDown(response.getTerm());
        } else {
            if (response.getSuccess()) {
                peer.setMatchIndex(prevLogIndex + numEntries);
                advanceCommitIndex();
                peer.setNextIndex(peer.getMatchIndex() + 1);
            } else {
                if (peer.getNextIndex() > 1) {
                    peer.setNextIndex(peer.getNextIndex() - 1);
                }
                if (response.getLastLogIndex() != 0
                        && peer.getNextIndex() > response.getLastLogIndex() + 1) {
                    peer.setNextIndex(response.getLastLogIndex() + 1);
                }
            }
        }
    }

    public void advanceCommitIndex() {
        // 获取quorum matchIndex
        int peerNum = peers.size();
        long[] matchIndexes = new long[peerNum + 1];
        for (int i = 0; i < peerNum - 1; i++) {
            matchIndexes[i] = peers.get(i).getMatchIndex();
        }
        matchIndexes[peerNum] = lastSyncedIndex;
        Arrays.sort(matchIndexes);
        long newCommitIndex = matchIndexes[(peerNum + 1 + 1) / 2];
        if (raftLog.getEntry(newCommitIndex).getTerm() != currentTerm) {
            return;
        }

        lock.lock();
        if (commitIndex >= newCommitIndex) {
            return;
        }
        long oldCommitIndex = commitIndex;
        commitIndex = newCommitIndex;
        // 同步到状态机
        for (long index = oldCommitIndex + 1; index <= newCommitIndex; index++) {
            Raft.LogEntry entry = raftLog.getEntry(index);
            stateMachine.apply(entry.getData().toByteArray());
        }
        lastAppliedIndex = commitIndex;
        commitIndexCondition.signalAll();
        lock.unlock();
    }

    public long packEntries(long nextIndex, Raft.AppendEntriesRequest.Builder requestBuilder) {
        long lastIndex = Math.min(raftLog.getLastLogIndex(),
                nextIndex + RaftOption.maxLogEntriesPerRequest - 1);
        for (long index = nextIndex; index <= lastIndex; index++) {
            Raft.LogEntry entry = raftLog.getEntry(index);
            requestBuilder.addEntries(entry);
        }
        return lastIndex - nextIndex + 1;
    }

    public void installSnapshot(Peer peer) {
        Raft.InstallSnapshotRequest.Builder requestBuilder = Raft.InstallSnapshotRequest.newBuilder();
        requestBuilder.setServerId(localServer.getServerId());
        requestBuilder.setTerm(currentTerm);
        // send snapshot
        try {
            snapshotLock.readLock().lock();
            Raft.InstallSnapshotRequest request = this.buildInstallSnapshotRequest(
                    null, 0, 0);
            peer.getRpcClient().asyncCall("RaftConsensusService.installSnapshot",
                    request, new InstallSnapshotResponseCallback(peer, request));
            isSnapshoting = true;
        } finally {
            snapshotLock.readLock().unlock();
        }

    }

    private class InstallSnapshotResponseCallback implements RPCCallback<Raft.InstallSnapshotResponse> {
        private Peer peer;
        private Raft.InstallSnapshotRequest request;

        public InstallSnapshotResponseCallback(Peer peer, Raft.InstallSnapshotRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(Raft.InstallSnapshotResponse response) {
            TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap = snapshot.getSnapshotDataFileMap();
            Snapshot.SnapshotDataFile lastDataFile = snapshotDataFileMap.get(request.getFileName());
            try {
                snapshotLock.readLock().lock();
                if (request.getIsLast() == true) {
                    isSnapshoting = false;
                } else {
                    Raft.InstallSnapshotRequest currentRequest = buildInstallSnapshotRequest(
                            this.request.getFileName(),
                            this.request.getOffset(),
                            this.request.getData().toByteArray().length);
                    peer.getRpcClient().asyncCall("RaftConsensusService.installSnapshot",
                            currentRequest, new InstallSnapshotResponseCallback(peer, currentRequest));
                }
            } finally {
                snapshotLock.readLock().unlock();
            }
        }

        @Override
        public void fail(Throwable e) {
            LOG.warn("install snapshot failed, msg={}", e.getMessage());
            snapshotLock.readLock().lock();
            isSnapshoting = false;
            snapshotLock.readLock().unlock();
        }
    }

    private Raft.InstallSnapshotRequest buildInstallSnapshotRequest(
            String lastFileName, long lastOffset, long lastLength) {
        Raft.InstallSnapshotRequest.Builder requestBuilder = Raft.InstallSnapshotRequest.newBuilder();
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
            int currentDataSize = RaftOption.maxSnapshotBytesPerRequest;
            Snapshot.SnapshotDataFile currentDataFile = lastFile;
            if (lastOffset + lastLength < lastFileLength) {
                if (lastOffset + lastLength + RaftOption.maxSnapshotBytesPerRequest > lastFileLength) {
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
                if (currentFileLenght < RaftOption.maxSnapshotBytesPerRequest) {
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

    public void becomeLeader() {
        state = NodeState.STATE_LEADER;
        leaderId = localServer.getServerId();
        // start heartbeat timer
        resetHeartbeatTimer();
    }

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
        resetElectionTimer();
    }

    public void updateMetaData() {
        raftLog.updateMetaData(currentTerm, votedFor, null);
    }

    public int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = RaftOption.electionTimeoutMilliseconds
                + random.nextInt(0, RaftOption.electionTimeoutMilliseconds);
        return randomElectionTimeout;
    }

    // client set command
    public void replicate(byte[] data) {
        Raft.LogEntry logEntry = Raft.LogEntry.newBuilder()
                .setTerm(currentTerm)
                .setType(Raft.EntryType.ENTRY_TYPE_DATA)
                .setData(ByteString.copyFrom(data)).build();
        List<Raft.LogEntry> entries = new ArrayList<>();
        entries.add(logEntry);
        long newLastLogIndex = raftLog.append(entries);
        for (final Peer peer : peers) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    appendEntries(peer);
                }
            });
        }
        // sync wait condition commitIndex >= newLastLogIndex
        // TODO: add timeout
        lock.lock();
        try {
            while (commitIndex < newLastLogIndex) {
                try {
                    commitIndexCondition.await();
                } catch (InterruptedException ex) {
                    LOG.warn(ex.getMessage());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void takeSnapshot() {
        if (raftLog.getTotalSize() < RaftOption.snapshotMinLogSize) {
            return;
        }
        if (lastAppliedIndex <= lastSnapshotIndex) {
            return;
        }
        long lastAppliedTerm = 0;
        if (lastAppliedIndex >= raftLog.getStartLogIndex()
                && lastAppliedIndex <= raftLog.getLastLogIndex()) {
            lastAppliedTerm = raftLog.getEntry(lastAppliedIndex).getTerm();
        }

        snapshotLock.writeLock().lock();
        // take snapshot
        String tmpSnapshotDir = snapshot.getSnapshotDir() + ".tmp";
        snapshot.updateMetaData(tmpSnapshotDir, lastAppliedIndex, lastAppliedTerm);
        String tmpSnapshotDataDir = tmpSnapshotDir + File.pathSeparator + "data";
        stateMachine.writeSnapshot(tmpSnapshotDataDir);
        try {
            FileUtils.moveDirectory(new File(tmpSnapshotDir), new File(snapshot.getSnapshotDir()));
            lastSnapshotIndex = lastAppliedIndex;
            lastSnapshotTerm = lastAppliedTerm;
        } catch (IOException ex) {
            LOG.warn("move direct failed, msg={}", ex.getMessage());
        }
        snapshotLock.writeLock().unlock();
    }

    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public List<Raft.LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<Raft.LogEntry> entries) {
        this.entries = entries;
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

    public ServerAddress getLocalServer() {
        return localServer;
    }

    public void setLocalServer(ServerAddress localServer) {
        this.localServer = localServer;
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

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public ReadWriteLock getSnapshotLock() {
        return snapshotLock;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }
}
