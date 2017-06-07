package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.ExampleMessage;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.util.JsonFormat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ConcurrentClientMain {
    private static JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

    public static void main(String[] args) {
        // parse args
        String ipPorts = args[0];
        RPCClient rpcClient = new RPCClient(ipPorts);
        ExampleService exampleService = RPCProxy.getProxy(rpcClient, ExampleService.class);

        long startTime = System.currentTimeMillis();
        // set
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(3);
        Future<?>[] future = new Future[3];
        for (int i = 0; i < 3; i++) {
            future[i] = writeThreadPool.submit(new SetTask(exampleService, i));
        }

        while (!future[0].isDone() || !future[1].isDone() || !future[2].isDone()) {
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
        for (int i = 0; i < 3; i++) {
            future[i] = readThreadPool.submit(new GetTask(exampleService, i));
        }

        while (!future[0].isDone() || !future[1].isDone() || !future[2].isDone()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        System.out.printf("read elapseMS=%d\n", System.currentTimeMillis() - startTime);
        rpcClient.stop();
    }

    public static class SetTask implements Runnable {
        private ExampleService exampleService;
        private int id;

        public SetTask(ExampleService exampleService, int id) {
            this.exampleService = exampleService;
            this.id = id;
        }

        @Override
        public void run() {
            for (int j = 0; j < 100000; j++) {
                String key = "hello" + id + j;
                String value = "world" + id + j;
                ExampleMessage.SetRequest setRequest = ExampleMessage.SetRequest.newBuilder()
                        .setKey(key).setValue(value).build();
                ExampleMessage.SetResponse setResponse = exampleService.set(setRequest);
                try {
                    if (setResponse != null) {
                        System.out.printf("set request, key=%s value=%s response=%s\n",
                                key, value, printer.print(setResponse));
                    } else {
                        System.out.printf("set request, key=%s value=%s response=null\n",
                                key, value);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class GetTask implements Runnable {
        private ExampleService exampleService;
        private int id;

        public GetTask(ExampleService exampleService, int id) {
            this.exampleService = exampleService;
            this.id = id;
        }

        @Override
        public void run() {
            for (int j = 0; j < 100000; j++) {
                String key = "hello" + id + j;
                ExampleMessage.GetRequest getRequest = ExampleMessage.GetRequest.newBuilder()
                        .setKey(key).build();
                ExampleMessage.GetResponse getResponse = exampleService.get(getRequest);
                try {
                    if (getResponse != null) {
                        System.out.printf("get request, key=%s, response=%s\n",
                                key, printer.print(getResponse));
                    } else {
                        System.out.printf("get request, key=%s, response=null\n", key);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

}
