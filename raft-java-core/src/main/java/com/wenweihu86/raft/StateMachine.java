package com.wenweihu86.raft;

/**
 * Created by wenweihu86 on 2017/5/10.
 */
public interface StateMachine {

    void writeSnapshot(String snapshotDir);

    void readSnapshot(String snapshotDir);

    void apply(byte[] dataBytes);
}
