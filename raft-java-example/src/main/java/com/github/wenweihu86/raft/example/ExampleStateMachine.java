package com.github.wenweihu86.raft.example;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.StateMachine;
import com.github.wenweihu86.raft.example.service.Example;
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

    @Override
    public void writeSnapshot(String snapshotDir) {
        String tmpDir = snapshotDir + ".tmp";
        Checkpoint checkpoint = Checkpoint.create(db);
        try {
            checkpoint.createCheckpoint(tmpDir);
            FileUtils.moveDirectory(new File(tmpDir), new File(snapshotDir));
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            // copy snapshot dir to data dir
            String dataDir = RaftOptions.dataDir + File.pathSeparator + "rocksdb_data";
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
            Example.SetRequest request = Example.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }

    public Example.GetResponse get(Example.GetRequest request) {
        try {
            byte[] keyBytes = request.getKey().getBytes();
            byte[] valueBytes = db.get(keyBytes);
            String value = new String(valueBytes);
            Example.GetResponse response = Example.GetResponse.newBuilder()
                    .setValue(value).build();
            return response;
        } catch (RocksDBException ex) {
            LOG.warn("read rockdb error, msg={}", ex.getMessage());
            return null;
        }
    }

}
