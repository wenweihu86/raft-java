# raft-java
Raft implementation library for Java.<br>
参考自[Raft论文](https://github.com/maemual/raft-zh_cn)和Raft作者的开源实现[LogCabin](https://github.com/logcabin/logcabin)。

# 支持的功能
* leader选举
* 日志复制
* snapshot
* 集群成员动态更变

## Quick Start
在本地单机上部署一套3实例的raft集群，执行如下脚本：<br>
cd raft-java-example && sh deploy.sh <br>
该脚本会在raft-java-example/env目录部署三个实例example1、example2、example3；<br>
同时会创建一个client目录，用于测试raft集群读写功能。<br>
部署成功后，测试写操作，通过如下脚本：
cd env/client <br>
./bin/run_client.sh "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello world <br>
测试读操作命令：<br>
./bin/run_client.sh "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello

# 使用方法
下面介绍如何在代码中使用raft-java依赖库来实现一套分布式存储系统。
## 配置依赖
```
<dependency>
    <groupId>com.github.wenweihu86.raft</groupId>
    <artifactId>raft-java-core</artifactId>
    <version>1.8.0</version>
</dependency>
```

## 定义数据写入和读取接口
```protobuf
message SetRequest {
    string key = 1;
    string value = 2;
}
message SetResponse {
    bool success = 1;
}
message GetRequest {
    string key = 1;
}
message GetResponse {
    string value = 1;
}
```
```java
public interface ExampleService {
    Example.SetResponse set(Example.SetRequest request);
    Example.GetResponse get(Example.GetRequest request);
}
```

## 服务端使用方法
1. 实现状态机StateMachine接口实现类
```java
// 该接口三个方法主要是给Raft内部调用
public interface StateMachine {
    /**
     * 对状态机中数据进行snapshot，每个节点本地定时调用
     * @param snapshotDir snapshot数据输出目录
     */
    void writeSnapshot(String snapshotDir);
    /**
     * 读取snapshot到状态机，节点启动时调用
     * @param snapshotDir snapshot数据目录
     */
    void readSnapshot(String snapshotDir);
    /**
     * 将数据应用到状态机
     * @param dataBytes 数据二进制
     */
    void apply(byte[] dataBytes);
}
```

2. 实现数据写入和读取接口
```
// ExampleService实现类中需要包含以下成员
private RaftNode raftNode;
private ExampleStateMachine stateMachine;
```
```
// 数据写入主要逻辑
byte[] data = request.toByteArray();
// 数据同步写入raft集群
boolean success = raftNode.replicate(data, Raft.EntryType.ENTRY_TYPE_DATA);
Example.SetResponse response = Example.SetResponse.newBuilder().setSuccess(success).build();
```
```
// 数据读取主要逻辑，由具体应用状态机实现
Example.GetResponse response = stateMachine.get(request);
```

3. 服务端启动逻辑
```
// 初始化RPCServer
RPCServer server = new RPCServer(localServer.getEndPoint().getPort());
// 应用状态机
ExampleStateMachine stateMachine = new ExampleStateMachine();
// 设置Raft选项，比如：
RaftOptions.snapshotMinLogSize = 10 * 1024;
RaftOptions.snapshotPeriodSeconds = 30;
RaftOptions.maxSegmentFileSize = 1024 * 1024;
// 初始化RaftNode
RaftNode raftNode = new RaftNode(serverList, localServer, stateMachine);
// 注册Raft节点之间相互调用的服务
RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
server.registerService(raftConsensusService);
// 注册给Client调用的Raft服务
RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
server.registerService(raftClientService);
// 注册应用自己提供的服务
ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
server.registerService(exampleService);
// 启动RPCServer，初始化Raft节点
server.start();
raftNode.init();
```
