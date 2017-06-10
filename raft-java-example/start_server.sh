#!/usr/bin/env bash

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example1/data -cp dependency/*:raft-java-example-1.2.0.jar com.github.wenweihu86.raft.example.server.ServerMain "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8051:1"

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example2/data -cp dependency/*:raft-java-example-1.2.0.jar com.github.wenweihu86.raft.example.server.ServerMain "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8052:2"

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example3/data -cp dependency/*:raft-java-example-1.2.0.jar com.github.wenweihu86.raft.example.server.ServerMain "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8053:3"

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example4/data -cp dependency/*:raft-java-example-1.2.0.jar com.github.wenweihu86.raft.example.server.ServerMain "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8054:4"

java -Dcom.github.wenweihu86.raft.data.dir=/Users/baidu/local/raft-java-example5/data -cp dependency/*:raft-java-example-1.2.0.jar com.github.wenweihu86.raft.example.server.ServerMain "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3" "127.0.0.1:8055:5"
