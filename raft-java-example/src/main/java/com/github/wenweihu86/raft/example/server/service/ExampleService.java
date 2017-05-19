package com.github.wenweihu86.raft.example.server.service;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public interface ExampleService {

    Example.SetResponse set(Example.SetRequest request);

    Example.GetResponse get(Example.GetRequest request);
}
