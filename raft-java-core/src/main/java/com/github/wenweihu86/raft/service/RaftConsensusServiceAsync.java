package com.github.wenweihu86.raft.service;

import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.rpc.client.RPCCallback;

import java.util.concurrent.Future;

/**
 * 用于生成client异步调用所需的proxy
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusServiceAsync extends RaftConsensusService {

    Future<RaftMessage.VoteResponse> preVote(
            RaftMessage.VoteRequest request,
            RPCCallback<RaftMessage.VoteResponse> callback);

    Future<RaftMessage.VoteResponse> requestVote(
            RaftMessage.VoteRequest request,
            RPCCallback<RaftMessage.VoteResponse> callback);

    Future<RaftMessage.AppendEntriesResponse> appendEntries(
            RaftMessage.AppendEntriesRequest request,
            RPCCallback<RaftMessage.AppendEntriesResponse> callback);

    Future<RaftMessage.InstallSnapshotResponse> installSnapshot(
            RaftMessage.InstallSnapshotRequest request,
            RPCCallback<RaftMessage.InstallSnapshotResponse> callback);
}
