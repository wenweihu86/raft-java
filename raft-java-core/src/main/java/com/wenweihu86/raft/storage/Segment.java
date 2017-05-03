package com.wenweihu86.raft.storage;

import com.wenweihu86.raft.proto.Raft;

import java.util.Deque;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class Segment {

    public class Record {
        public long offset;
        public Raft.LogEntry entry;
    }

    private boolean isOpen;
    private long startIndex;
    private long endIndex;
    private long fileSize;
    private String fileName;
    private Deque<Record> entries;

    public boolean isOpen() {
        return isOpen;
    }

    public void setOpen(boolean open) {
        isOpen = open;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Deque<Record> getEntries() {
        return entries;
    }

    public void setEntries(Deque<Record> entries) {
        this.entries = entries;
    }
}
