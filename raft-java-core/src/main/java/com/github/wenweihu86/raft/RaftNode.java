package com.github.wenweihu86.raft;

import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.storage.SegmentedLog;
import com.google.protobuf.ByteString;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.wenweihu86.rpc.client.RPCCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    private Lock lock = new ReentrantLock();
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
    private long lastApplied;
    private long lastSnapshotIndex;
    private long lastSnapshotTerm;
    private List<Peer> peers;
    private ServerAddress localServer;
    private int leaderId; // leader节点id
    private SegmentedLog raftLog;
    private Snapshot snapshot;
    private StateMachine stateMachine;

    private Lock commitIndexLock = new ReentrantLock();
    private Condition commitIndexCondition = commitIndexLock.newCondition();

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
    }

    public void init() {
        this.currentTerm = raftLog.getMetaData().getCurrentTerm();
        this.votedFor = raftLog.getMetaData().getVotedFor();
        this.commitIndex = Math.max(snapshot.getMetaData().getLastIncludedIndex(), commitIndex);
        stepDown(1);
    }

    public void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(false);
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

        commitIndexLock.lock();
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
        commitIndexCondition.signalAll();
        commitIndexLock.unlock();
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
        // TODO: send snapshot
    }

    public void becomeLeader() {
        state = NodeState.STATE_LEADER;
        leaderId = localServer.getServerId();
        // start heartbeat timer
        resetHeartbeatTimer();
    }

    public void stepDown(long newTerm) {
        assert this.currentTerm <= newTerm;
        if (this.currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = -1;
            votedFor = -1;
            state = NodeState.STATE_FOLLOWER;
            raftLog.updateMetaData(currentTerm, votedFor, null);
        } else {
            if (state != NodeState.STATE_FOLLOWER) {
                state = NodeState.STATE_FOLLOWER;
            }
        }
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
        commitIndexLock.lock();
        try {
            while (commitIndex < newLastLogIndex) {
                try {
                    commitIndexCondition.await();
                } catch (InterruptedException ex) {
                    LOG.warn(ex.getMessage());
                }
            }
        } finally {
            commitIndexLock.unlock();
        }
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

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
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
}
