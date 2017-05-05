package com.wenweihu86.raft.storage;

import com.wenweihu86.raft.proto.Raft;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class Segment {

    public static class Record {
        public long offset;
        public Raft.LogEntry entry;
        public Record(long offset, Raft.LogEntry entry) {
            this.offset = offset;
            this.entry = entry;
        }
    }

    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    private long fileSize;
    private String fileName;
    private RandomAccessFile randomAccessFile;
    private List<Record> entries = new ArrayList<>();

    public Raft.LogEntry getEntry(long index) {
        if (startIndex == -1 || endIndex == -1) {
            return null;
        }
        if (index < startIndex || index > endIndex) {
            return null;
        }
        int indexInList = (int) (index - startIndex);
        return entries.get(indexInList).entry;
    }

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
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

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public void setRandomAccessFile(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public List<Record> getEntries() {
        return entries;
    }

    public void setEntries(List<Record> entries) {
        this.entries = entries;
    }
}
