package com.wenweihu86.raft;

import com.wenweihu86.raft.proto.Raft;
import com.wenweihu86.raft.storage.SegmentedLog;
import com.wenweihu86.rpc.client.RPCCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
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
    // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private long lastApplied;
    private List<Peer> peers;
    private ServerAddress localServer;
    private int leaderId; // leader节点id
    private SegmentedLog raftLog;

    private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture voteScheduledFuture;

    public RaftNode(int localServerId, List<ServerAddress> servers) {
        for (ServerAddress server : servers) {
            if (server.getServerId() == localServerId) {
                this.localServer = server;
            } else {
                Peer peer = new Peer(server);
                peers.add(peer);
            }
        }

        raftLog = new SegmentedLog();
        voteScheduledFuture = scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // TODO
            }
        }, 0, getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
        stepDown(1);
    }

    public void init() {
        this.currentTerm = raftLog.getMetaData().getCurrentTerm();
        this.votedFor = raftLog.getMetaData().getVotedFor();
    }

    public void startNewElection() {
        currentTerm++;
        state = NodeState.STATE_CANDIDATE;
        leaderId = 0;
        votedFor = localServer.getServerId();
        // TODO: requestVote to peers
    }

    public void requestVote() {
        Raft.VoteRequest request = Raft.VoteRequest.newBuilder()
                .setServerId(localServer.getServerId())
                .setTerm(currentTerm)
                .setLastLogIndex(raftLog.getLastLogIndex())
                .setLastLogTerm(getLastLogTerm()).build();
        for (Peer peer : peers) {
            peer.getRpcClient().asyncCall(
                    "RaftApi.requestVote", request,
                    new VoteResponseCallback(peer));
        }
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

    public void becomeLeader() {
        state = NodeState.STATE_LEADER;
        leaderId = localServer.getServerId();
        // TODO: send AppendEntries to peers
    }

    public long getLastLogTerm() {
        long lastLogIndex = raftLog.getLastLogIndex();
        return raftLog.getEntry(lastLogIndex).getTerm();
    }

    public long getLastLogIndex() {
        return raftLog.getLastLogIndex();
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
}
