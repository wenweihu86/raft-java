package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.ExampleMessage;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.util.JsonFormat;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ConcurrentClientMain {
    private static JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.printf("Usage: ./run_concurrent_client.sh THREAD_NUM\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        RPCClient rpcClient = new RPCClient(ipPorts);
        ExampleService exampleService = RPCProxy.getProxy(rpcClient, ExampleService.class);

        ExecutorService readThreadPool = Executors.newFixedThreadPool(3);
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(3);
        Future<?>[] future = new Future[3];
        for (int i = 0; i < 3; i++) {
            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool));
        }
    }

    public static class SetTask implements Runnable {
        private ExampleService exampleService;
        ExecutorService readThreadPool;

        public SetTask(ExampleService exampleService, ExecutorService readThreadPool) {
            this.exampleService = exampleService;
            this.readThreadPool = readThreadPool;
        }

        @Override
        public void run() {
            while (true) {
                String key = UUID.randomUUID().toString();
                String value = UUID.randomUUID().toString();
                ExampleMessage.SetRequest setRequest = ExampleMessage.SetRequest.newBuilder()
                        .setKey(key).setValue(value).build();

                long startTime = System.currentTimeMillis();
                ExampleMessage.SetResponse setResponse = exampleService.set(setRequest);
                try {
                    if (setResponse != null) {
                        System.out.printf("set request, key=%s, value=%s, response=%s, elapseMS=%d\n",
                                key, value, printer.print(setResponse), System.currentTimeMillis() - startTime);
                        readThreadPool.submit(new GetTask(exampleService, key));
                    } else {
                        System.out.printf("set request failed, key=%s value=%s\n", key, value);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static class GetTask implements Runnable {
        private ExampleService exampleService;
        private String key;

        public GetTask(ExampleService exampleService, String key) {
            this.exampleService = exampleService;
            this.key = key;
        }

        @Override
        public void run() {
            ExampleMessage.GetRequest getRequest = ExampleMessage.GetRequest.newBuilder()
                    .setKey(key).build();
            long startTime = System.currentTimeMillis();
            ExampleMessage.GetResponse getResponse = exampleService.get(getRequest);
            try {
                if (getResponse != null) {
                    System.out.printf("get request, key=%s, response=%s, elapseMS=%d\n",
                            key, printer.print(getResponse), System.currentTimeMillis() - startTime);
                } else {
                    System.out.printf("get request failed, key=%s\n", key);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
