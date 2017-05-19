package com.github.wenweihu86.raft.example.server.service.impl;

import com.github.wenweihu86.raft.example.server.ExampleStateMachine;
import com.github.wenweihu86.raft.example.server.service.Example;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.RaftNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ExampleServiceImpl implements ExampleService {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceImpl.class);
    private static JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

    private RaftNode raftNode;
    private ExampleStateMachine stateMachine;

    public ExampleServiceImpl(RaftNode raftNode, ExampleStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    @Override
    public Example.SetResponse set(Example.SetRequest request) {
        byte[] data = request.toByteArray();
        boolean success = raftNode.replicate(data);
        Example.SetResponse response = Example.SetResponse.newBuilder()
                .setSuccess(success).build();
        try {
            LOG.info("set request, request={}, response={}", printer.print(request),
                    printer.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }

        return response;
    }

    @Override
    public Example.GetResponse get(Example.GetRequest request) {
        Example.GetResponse response = stateMachine.get(request);
        try {
            LOG.info("get request, request={}, response={}", printer.print(request),
                    printer.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
        return response;
    }

}
