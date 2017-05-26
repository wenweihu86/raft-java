#!/usr/bin/env bash

serverId="$1"
cluster="127.0.0.1:8050:1,127.0.0.1:8051:2,127.0.0.1:8052:3"
if [ $serverId -eq "1" ]; then
    localServer="127.0.0.1:8050:1"
elif [ $serverId -eq "2" ]; then
    localServer="127.0.0.1:8051:2"
elif [ $serverId -eq "3" ]; then
    localServer="127.0.0.1:8052:3"
fi

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example${serverId}/data -cp dependency/*:raft-java-example-1.0.0-SNAPSHOT.jar com.github.wenweihu86.raft.example.server.ServerMain ${cluster} ${localServer}
