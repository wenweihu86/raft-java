package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.ExampleMessage;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.google.protobuf.util.JsonFormat;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ClientMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.printf("Usage: ./run_server.sh CLUSTER KEY [VALUE]\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        String key = args[1];
        String value = null;
        if (args.length > 2) {
            value = args[2];
        }

        // init rpc client
        RPCClient rpcClient = new RPCClient(ipPorts);
        ExampleService exampleService = RPCProxy.getProxy(rpcClient, ExampleService.class);
        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

        // set
        if (value != null) {
            ExampleMessage.SetRequest setRequest = ExampleMessage.SetRequest.newBuilder()
                    .setKey(key).setValue(value).build();
            ExampleMessage.SetResponse setResponse = exampleService.set(setRequest);
            try {
                System.out.printf("set request, key=%s value=%s response=%s\n",
                        key, value, printer.print(setResponse));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            // get
            ExampleMessage.GetRequest getRequest = ExampleMessage.GetRequest.newBuilder()
                    .setKey(key).build();
            ExampleMessage.GetResponse getResponse = exampleService.get(getRequest);
            try {
                System.out.printf("get request, key=%s, response=%s\n",
                        key, printer.print(getResponse));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        rpcClient.stop();
    }
}
