package com.github.wenweihu86.raft.example;

import com.github.wenweihu86.raft.example.service.Example;
import com.github.wenweihu86.raft.example.service.ExampleService;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.google.protobuf.util.JsonFormat;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ClientMain {
    public static void main(String[] args) {
        String ipPorts = "127.0.0.1:8050;127.0.0.1:8051;127.0.0.1:8052";
        if (args.length == 1) {
            ipPorts = args[0];
        }
        RPCClient rpcClient = new RPCClient(ipPorts);
        ExampleService exampleService = new ExampleServiceProxy(rpcClient);
        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        // set
        Example.SetRequest setRequest = Example.SetRequest.newBuilder()
                .setKey("hello").setValue("raft").build();
        Example.SetResponse setResponse = exampleService.set(setRequest);
        try {
            System.out.println(printer.print(setResponse));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

        // get
        Example.GetRequest getRequest = Example.GetRequest.newBuilder()
                .setKey("hello").build();
        Example.GetResponse getResponse = exampleService.get(getRequest);
        try {
            System.out.println(printer.print(getResponse));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}
