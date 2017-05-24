package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.Example;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ExampleServiceProxy implements ExampleService {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceProxy.class);
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    private List<Raft.Server> cluster;
    private RPCClient clusterRPCClient;
    private ExampleService clusterExampleService;
    private RaftClientService clusterRaftClientService;

    private Raft.Server leader;
    private RPCClient leaderRPCClient;
    private RaftClientService leaderRaftClientService;
    private ExampleService leaderExampleService;

    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public ExampleServiceProxy(String ipPorts) {
        this.clusterRPCClient = new RPCClient(ipPorts);
        clusterExampleService = RPCProxy.getProxy(clusterRPCClient, ExampleService.class);
        clusterRaftClientService = RPCProxy.getProxy(clusterRPCClient, RaftClientService.class);
        timer.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateConfiguration();
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    @Override
    public Example.SetResponse set(Example.SetRequest request) {
        Example.SetResponse response = Example.SetResponse.newBuilder()
                .setSuccess(false).build();
        readWriteLock.readLock().lock();
        try {
            if (leader == null) {
                readWriteLock.readLock().unlock();
                try {
                    updateConfiguration();
                } finally {
                    readWriteLock.readLock().lock();
                }
            }
            if (leader != null) {
                response = leaderExampleService.set(request);
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }

        try {
            LOG.info("set request={} response={}",
                    PRINTER.print(request), PRINTER.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        }
        return response;
    }

    @Override
    public Example.GetResponse get(Example.GetRequest request) {
        Example.GetResponse response = Example.GetResponse.newBuilder().build();
        readWriteLock.readLock().lock();
        try {
            response = clusterExampleService.get(request);
            LOG.info("get request={} response={}",
                    PRINTER.print(request), PRINTER.print(response));
        } catch (InvalidProtocolBufferException ex) {
            ex.printStackTrace();
        } finally {
            readWriteLock.readLock().unlock();
        }
        return response;
    }

    private void updateConfiguration() {
        Raft.GetConfigurationRequest request = Raft.GetConfigurationRequest.newBuilder().build();
        Raft.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);
        if (response != null && response.getResCode() == Raft.ResCode.RES_CODE_SUCCESS) {
            readWriteLock.writeLock().lock();
            try {
                leaderRPCClient.stop();
                leader = response.getLeader();
                leaderRPCClient = new RPCClient(convertEndPoint(leader.getEndPoint()));
                leaderRaftClientService = RPCProxy.getProxy(leaderRPCClient, RaftClientService.class);
                leaderExampleService = RPCProxy.getProxy(leaderRPCClient, ExampleService.class);

                clusterRPCClient.stop();
                cluster = response.getServersList();
                clusterRPCClient = new RPCClient(convertEndPoints(cluster));
                clusterRaftClientService = RPCProxy.getProxy(clusterRPCClient, RaftClientService.class);
                clusterExampleService = RPCProxy.getProxy(clusterRPCClient, ExampleService.class);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private EndPoint convertEndPoint(Raft.EndPoint endPoint) {
        return new EndPoint(endPoint.getHost(), endPoint.getPort());
    }

    private List<EndPoint> convertEndPoints(List<Raft.Server> servers) {
        List<EndPoint> endPoints = new ArrayList<>(servers.size());
        for (Raft.Server server : servers) {
            endPoints.add(convertEndPoint(server.getEndPoint()));
        }
        return endPoints;
    }

}
