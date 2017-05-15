package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.github.wenweihu86.raft.proto.Raft;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir = RaftOptions.dataDir + File.separator + "log";
    private String logDataDir = logDir + File.separator + "data";
    private Raft.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
    // segment log占用的内存大小，用于判断是否需要做snapshot
    private volatile long totalSize;

    public SegmentedLog() {
        File file = new File(logDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments();
        for (Segment segment : startLogIndexSegmentMap.values()) {
            this.loadSegmentData(segment);
        }

        metaData = this.readMetaData();
        if (metaData == null) {
            if (startLogIndexSegmentMap.size() > 0) {
                LOG.error("No readable metadata file but found segments in {}", logDir);
                throw new RuntimeException("No readable metadata file but found segments");
            }
            metaData = Raft.LogMetaData.newBuilder().setFirstLogIndex(0).build();
        }
    }

    public Raft.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            LOG.warn("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        Segment segment = startLogIndexSegmentMap.lowerEntry(index).getValue();
        return segment.getEntry(index);
    }

    public long getFirstLogIndex() {
        if (startLogIndexSegmentMap.size() == 0) {
            return 0;
        }
        Segment firstSegment = startLogIndexSegmentMap.firstEntry().getValue();
        return firstSegment.getStartIndex();
    }

    public long getLastLogIndex() {
        if (startLogIndexSegmentMap.size() == 0) {
            return 0;
        }
        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
        return lastSegment.getEndIndex();
    }

    public long getLastLogTerm() {
        long lastLogIndex = this.getLastLogIndex();
        if (lastLogIndex == 0) {
            return 0;
        }
        return this.getEntry(lastLogIndex).getTerm();
    }

    public long append(List<Raft.LogEntry> entries) {
        long newLastLogIndex = this.getLastLogIndex();
        for (Raft.LogEntry entry : entries) {
            newLastLogIndex++;
            int entrySize = entry.getSerializedSize();
            int segmentSize = startLogIndexSegmentMap.size();
            boolean isNeedNewSegmentFile = false;
            try {
                if (segmentSize == 0) {
                    isNeedNewSegmentFile = true;
                } else {
                    Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
                    if (!segment.isCanWrite()) {
                        isNeedNewSegmentFile = true;
                    } else if (segment.getFileSize() + entrySize >= RaftOptions.maxSegmentFileSize) {
                        isNeedNewSegmentFile = true;
                        // 最后一个segment的文件close并改名
                        segment.getRandomAccessFile().close();
                        segment.setCanWrite(false);
                        String newFileName = String.format("%020d-%020d",
                                segment.getStartIndex(), segment.getEndIndex());
                        File newFile = new File(newFileName);
                        newFile.createNewFile();
                        File oldFile = new File(segment.getFileName());
                        oldFile.renameTo(newFile);
                        segment.setFileName(newFileName);
                        segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newFileName, "r"));
                    }
                }
                Segment newSegment = startLogIndexSegmentMap.lastEntry().getValue();
                // 新建segment文件
                if (isNeedNewSegmentFile) {
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d", newLastLogIndex);
                    File newSegmentFile = new File(newSegmentFileName);
                    if (!newSegmentFile.exists()) {
                        newSegmentFile.createNewFile();
                    }
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(newLastLogIndex);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));
                    newSegment = segment;
                }
                // 写proto到segment中
                if (entry.getIndex() == 0) {
                    entry = Raft.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).build();
                }
                newSegment.setEndIndex(entry.getIndex());
                newSegment.getEntries().add(new Segment.Record(
                        newSegment.getRandomAccessFile().getFilePointer(), entry));
                RaftFileUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());
                if (startLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) {
                    startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                }
                totalSize += entrySize;
            }  catch (IOException ex) {
                throw new RuntimeException("meet exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }

    public void truncatePrefix(long newFirstIndex) {
        if (newFirstIndex <= getFirstLogIndex()) {
            return;
        }
        LOG.info("Truncating log from old first index {} to new first index {}",
                getFirstLogIndex(), newFirstIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.firstEntry().getValue();
            if (newFirstIndex <= segment.getEndIndex()) {
                List<Segment.Record> newEntries = new ArrayList<>();
                List<Segment.Record> oldEntries = segment.getEntries();
                int oldEntrySize = oldEntries.size();
                long newFirstOffset = 0;
                for (int index = (int) (newFirstIndex - segment.getStartIndex());
                     index < oldEntrySize; index++) {
                    Segment.Record record = segment.getEntries().get(index);
                    if (newFirstOffset == 0) {
                        newFirstOffset = record.offset;
                    }
                    record.offset -= newFirstOffset;
                    newEntries.add(record);
                }
                segment.setEntries(newEntries);
                segment.setStartIndex(newFirstIndex);
                segment.setFileSize(segment.getFileSize() - newFirstOffset);

                // 截取文件后半部分
                FileInputStream oldFileStream = null;
                FileOutputStream newFileStream = null;
                try {
                    File oldFile = new File(logDataDir + File.separator + segment.getFileName());
                    oldFileStream = new FileInputStream(oldFile);

                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());
                    File newFile = new File(logDataDir + File.separator + newFileName);
                    newFile.createNewFile();
                    newFileStream = new FileOutputStream(newFile);

                    IOUtils.copyLarge(oldFileStream, newFileStream, newFirstOffset, segment.getFileSize());

                    segment.setFileName(newFileName);
                    RaftFileUtils.closeFile(segment.getRandomAccessFile());
                    oldFile.delete();
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));
                    segment.setCanWrite(false);
                } catch (Exception ex) {
                    LOG.warn("exception, msg={}", ex.getMessage());
                } finally {
                    RaftFileUtils.closeFile(oldFileStream);
                    RaftFileUtils.closeFile(newFileStream);
                }
                break;
            } else if (newFirstIndex > segment.getEndIndex()){
                File oldFile = new File(logDataDir + File.separator + segment.getFileName());
                oldFile.delete();
                startLogIndexSegmentMap.remove(segment.getStartIndex());
            }
        }
        updateMetaData(null, null, newFirstIndex);
    }

    public void truncateSuffix(long newEndIndex) {
        if (newEndIndex >= getLastLogIndex()) {
            return;
        }
        LOG.info("Truncating log from old end index {} to new end index {}",
                getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            try {
                if (newEndIndex == segment.getEndIndex()) {
                    break;
                } else if (newEndIndex < segment.getStartIndex()) {
                    totalSize -= segment.getFileSize();
                    // delete file
                    segment.getRandomAccessFile().close();
                    String fullFileName = logDataDir + File.separator + segment.getFileName();
                    FileUtils.forceDelete(new File(fullFileName));
                    startLogIndexSegmentMap.remove(segment.getFileName());
                } else if (newEndIndex < segment.getEndIndex()) {
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                    segment.setEndIndex(newEndIndex);
                    long newFileSize = segment.getEntries().get(i).offset;
                    totalSize -= (segment.getFileSize() - newFileSize);
                    segment.setFileSize(newFileSize);
                    segment.getEntries().removeAll(
                            segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel fileChannel = segment.getRandomAccessFile().getChannel();
                    fileChannel.truncate(segment.getFileSize());
                    fileChannel.close();
                    segment.getRandomAccessFile().close();
                    String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());
                    segment.setFileName(newFileName);
                    String newFullFileName = logDataDir + File.separator + segment.getFileName();
                    new File(oldFullFileName).renameTo(new File(newFullFileName));
                    segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));
                }
            } catch (IOException ex) {
                LOG.warn("io exception, msg={}", ex.getMessage());
            }
        }
    }

    public void loadSegmentData(Segment segment) {
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0;
            while (offset < totalLength) {
                Raft.LogEntry entry = RaftFileUtils.readProtoFromFile(randomAccessFile, Raft.LogEntry.class);
                Segment.Record record = new Segment.Record(offset, entry);
                segment.getEntries().add(record);
                offset = randomAccessFile.getFilePointer();
            }
            totalSize += totalLength;
        } catch (Exception ex) {
            LOG.error("read segment meet exception, msg={}", ex.getMessage());
            throw new RuntimeException("file not found");
        }

        int entrySize = segment.getEntries().size();
        if (entrySize > 0) {
            segment.setStartIndex(segment.getEntries().get(0).entry.getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).entry.getIndex());
        }
    }

    public void readSegments() {
        List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(logDataDir);
        for (String fileName: fileNames) {
            String[] splitArray = fileName.split("-");
            if (splitArray.length != 2) {
                LOG.warn("segment filename[{}] is not valid", fileName);
                continue;
            }
            Segment segment = new Segment();
            segment.setFileName(fileName);
            try {
                if (splitArray[0].equals("open")) {
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.valueOf(splitArray[1]));
                    segment.setEndIndex(0);
                } else {
                    try {
                        segment.setCanWrite(false);
                        segment.setStartIndex(Long.parseLong(splitArray[0]));
                        segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException ex) {
                        LOG.warn("segment filename[{}] is not valid", fileName);
                        continue;
                    }
                }
                segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, fileName, "r"));
                segment.setFileSize(segment.getRandomAccessFile().length());
                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            } catch (IOException ioException) {
                LOG.warn("open segment file error, file={}, msg={}",
                        fileName, ioException.getMessage());
                throw new RuntimeException("open segment file error");
            }
        }
    }

    public Raft.LogMetaData readMetaData() {
        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            Raft.LogMetaData metadata = RaftFileUtils.readProtoFromFile(randomAccessFile, Raft.LogMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex) {
        Raft.LogMetaData.Builder builder = Raft.LogMetaData.newBuilder(this.metaData);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }
        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }
        this.metaData = builder.build();

        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            RaftFileUtils.writeProtoToFile(randomAccessFile, metaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

    public Raft.LogMetaData getMetaData() {
        return metaData;
    }

    public long getTotalSize() {
        return totalSize;
    }

}
