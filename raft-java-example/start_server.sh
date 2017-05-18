#!/usr/bin/env bash

serverId=$1

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example${serverId}/data -cp dependency/*:raft-java-example-1.0.0-SNAPSHOT.jar com.github.wenweihu86.raft.example.ServerMain "127.0.0.1:8050:1,127.0.0.1:8051:2,127.0.0.1:8052:3" ${serverId}
