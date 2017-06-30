#!/usr/bin/env bash

java -cp dependency/*:raft-java-example-1.3.0.jar com.github.wenweihu86.raft.example.client.ClientMain "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello raft

java -cp dependency/*:raft-java-example-1.3.0.jar com.github.wenweihu86.raft.example.client.ConcurrentClientMain "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053"
