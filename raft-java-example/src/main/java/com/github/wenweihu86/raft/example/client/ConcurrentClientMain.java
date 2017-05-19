package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.Example;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.google.protobuf.util.JsonFormat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ConcurrentClientMain {
    public static void main(String[] args) {
        // parse args
        String ipPorts = args[0];

        // init rpc client
        RPCClient rpcClient = new RPCClient(ipPorts);
        final ExampleService exampleService = new ExampleServiceProxy(rpcClient);

        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        long startTime = System.currentTimeMillis();
        // set
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(4);
        Future<?>[] future = new Future[2];
        for (int i = 0; i < 2; i++) {
            future[i] = writeThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100000; j++) {
                        String key = "hello" + j;
                        String value = "world" + j;
                        Example.SetRequest setRequest = Example.SetRequest.newBuilder()
                                .setKey(key).setValue(value).build();
                        Example.SetResponse setResponse = exampleService.set(setRequest);
                        try {
                            System.out.printf("set request, key=%s value=%s response=%s\n",
                                    key, value, printer.print(setResponse));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            });
        }

        while (!future[0].isDone() || future[1].isDone()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        System.out.printf("write elapseMS=%d\n", System.currentTimeMillis() - startTime);

        try {
            Thread.sleep(30000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        // get
        startTime = System.currentTimeMillis();
        ExecutorService readThreadPool = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 2; i++) {
            future[i] = readThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100000; j++) {
                        String key = "hello" + j;
                        Example.GetRequest getRequest = Example.GetRequest.newBuilder()
                                .setKey(key).build();
                        Example.GetResponse getResponse = exampleService.get(getRequest);
                        try {
                            System.out.printf("get request, key=%s, response=%s\n",
                                    key, printer.print(getResponse));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            });
        }

        while (!future[0].isDone() || future[1].isDone()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        System.out.printf("read elapseMS=%d\n", System.currentTimeMillis() - startTime);
    }

}
