package com.github.wenweihu86.raft.service;

import com.baidu.brpc.client.RpcCallback;
import com.github.wenweihu86.raft.proto.RaftProto;

import java.util.concurrent.Future;

/**
 * 用于生成client异步调用所需的proxy
 * Created by wenweihu86 on 2017/5/2.
 */
public interface RaftConsensusServiceAsync extends RaftConsensusService {

    Future<RaftProto.VoteResponse> preVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);

    Future<RaftProto.VoteResponse> requestVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);

    Future<RaftProto.AppendEntriesResponse> appendEntries(
            RaftProto.AppendEntriesRequest request,
            RpcCallback<RaftProto.AppendEntriesResponse> callback);

    Future<RaftProto.InstallSnapshotResponse> installSnapshot(
            RaftProto.InstallSnapshotRequest request,
            RpcCallback<RaftProto.InstallSnapshotResponse> callback);
}
