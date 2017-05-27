package com.github.wenweihu86.raft.admin;

import com.github.wenweihu86.raft.proto.Raft;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/14.
 */
public class AdminMain {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    public static void main(String[] args) {
        // parse args
        if (args.length < 3) {
            System.out.println("java -jar AdminMain servers cmd subCmd [args]");
            System.exit(1);
        }
        // servers format is like "10.1.1.1:8010:1,10.2.2.2:8011:2,10.3.3.3.3:8012:3"
        String servers = args[0];
        String cmd = args[1];
        String subCmd = args[2];
        Validate.isTrue(cmd.equals("conf"));
        Validate.isTrue(subCmd.equals("get")
                || subCmd.equals("add")
                || subCmd.equals("del"));
        RaftClientService client = new RaftClientServiceProxy(servers);
        if (subCmd.equals("get")) {
            Raft.GetConfigurationRequest request = Raft.GetConfigurationRequest.newBuilder().build();
            Raft.GetConfigurationResponse response = client.getConfiguration(request);
            try {
                if (response != null) {
                    System.out.println(PRINTER.print(response));
                } else {
                    System.out.printf("response == null");
                }
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }

        } else if (subCmd.equals("add")) {
            List<Raft.Server> serverList = parseServers(args[3]);
            Raft.AddPeersRequest request = Raft.AddPeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            Raft.AddPeersResponse response = client.addPeers(request);
            try {
                if (response != null) {
                    System.out.println(PRINTER.print(response));
                } else {
                    System.out.printf("response == null");
                }
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }
        } else if (subCmd.equals("del")) {
            List<Raft.Server> serverList = parseServers(args[3]);
            Raft.RemovePeersRequest request = Raft.RemovePeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            Raft.RemovePeersResponse response = client.removePeers(request);
            try {
                if (response != null) {
                    System.out.println(PRINTER.print(response));
                } else {
                    System.out.printf("response == null");
                }
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }
        }
        ((RaftClientServiceProxy) client).stop();
    }

    public static List<Raft.Server> parseServers(String serversString) {
        List<Raft.Server> serverList = new ArrayList<>();
        String[] splitArray1 = serversString.split(",");
        for (String addr : splitArray1) {
            String[] splitArray2 = addr.split(":");
            Raft.EndPoint endPoint = Raft.EndPoint.newBuilder()
                    .setHost(splitArray2[0])
                    .setPort(Integer.parseInt(splitArray2[1])).build();
            Raft.Server server = Raft.Server.newBuilder()
                    .setEndPoint(endPoint)
                    .setServerId(Integer.parseInt(splitArray2[2])).build();
            serverList.add(server);
        }
        return serverList;
    }
}
