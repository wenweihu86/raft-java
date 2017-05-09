package com.wenweihu86.raft.example.service.impl;

import com.wenweihu86.raft.RaftNode;
import com.wenweihu86.raft.example.ExampleStateMachine;
import com.wenweihu86.raft.example.service.Example;
import com.wenweihu86.raft.example.service.ExampleService;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ExampleServiceImpl implements ExampleService {

    private RaftNode raftNode;

    private ExampleStateMachine stateMachine;

    public ExampleServiceImpl(RaftNode raftNode, ExampleStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    @Override
    public Example.SetResponse set(Example.SetRequest request) {
        byte[] data = request.toByteArray();
        raftNode.replicate(data);
        return null;
    }

    @Override
    public Example.GetResponse get(Example.GetRequest request) {
        return stateMachine.get(request);
    }

}
