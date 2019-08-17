package com.github.wenweihu86.raft.example.server;

import com.github.wenweihu86.raft.StateMachine;
import com.github.wenweihu86.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by wenweihu86 on 2017/5/9.
 */
public class ExampleStateMachine implements StateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleStateMachine.class);

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private String raftDataDir;

    public ExampleStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }

    @Override
    public void writeSnapshot(String snapshotDir) {
        Checkpoint checkpoint = Checkpoint.create(db);
        try {
            checkpoint.createCheckpoint(snapshotDir);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.warn("writeSnapshot meet exception, dir={}, msg={}",
                    snapshotDir, ex.getMessage());
        }
    }

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            // copy snapshot dir to data dir
            if (db != null) {
                db.close();
            }
            String dataDir = raftDataDir + File.separator + "rocksdb_data";
            File dataFile = new File(dataDir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }
            File snapshotFile = new File(snapshotDir);
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile, dataFile);
            }
            // open rocksdb data dir
            Options options = new Options();
            options.setCreateIfMissing(true);
            db = RocksDB.open(options, dataDir);
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }

    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        try {
            ExampleProto.GetResponse.Builder responseBuilder = ExampleProto.GetResponse.newBuilder();
            byte[] keyBytes = request.getKey().getBytes();
            byte[] valueBytes = db.get(keyBytes);
            if (valueBytes != null) {
                String value = new String(valueBytes);
                responseBuilder.setValue(value);
            }
            ExampleProto.GetResponse response = responseBuilder.build();
            return response;
        } catch (RocksDBException ex) {
            LOG.warn("read rockdb error, msg={}", ex.getMessage());
            return null;
        }
    }

}
