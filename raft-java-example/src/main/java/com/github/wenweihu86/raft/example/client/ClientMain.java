package com.github.wenweihu86.raft.example.client;

import com.github.wenweihu86.raft.example.server.service.Example;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.google.protobuf.util.JsonFormat;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class ClientMain {
    public static void main(String[] args) {
        // parse args
        String ipPorts = args[0];
        String key = args[1];
        String value = null;
        if (args.length > 2) {
            value = args[2];
        }

        // init rpc client
        ExampleService exampleService = new ExampleServiceProxy(ipPorts);
        final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

        // set
        if (value != null) {
            Example.SetRequest setRequest = Example.SetRequest.newBuilder()
                    .setKey(key).setValue(value).build();
            Example.SetResponse setResponse = exampleService.set(setRequest);
            try {
                System.out.printf("set request, key=%s value=%s response=%s\n",
                        key, value, printer.print(setResponse));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            // get
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

        ((ExampleServiceProxy) exampleService).stop();
    }
}
