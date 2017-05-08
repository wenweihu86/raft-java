package com.wenweihu86.raft.storage;

import com.wenweihu86.raft.RaftOption;
import com.wenweihu86.raft.proto.Raft;
import com.wenweihu86.raft.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir = RaftOption.dataDir + File.pathSeparator + "log";
    private Raft.LogMetaData metaData;
    private List<Segment> segments = new ArrayList<>();
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
    private AtomicLong openedSegmentIndex = new AtomicLong(0);

    public SegmentedLog() {
        File file = new File(logDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        segments = this.readSegments();
        for (Segment segment : segments) {
            this.loadSegmentData(segment);
        }

        metaData = this.readMetaData();
        if (metaData == null) {
            if (segments.size() > 0) {
                LOG.error("No readable metadata file but found segments in {}", logDir);
                throw new RuntimeException("No readable metadata file but found segments");
            }
            metaData = Raft.LogMetaData.newBuilder().setStartLogIndex(0).build();
        }
    }

    public Raft.LogEntry getEntry(long index) {
        long startLogIndex = getStartLogIndex();
        long lastLogIndex = getLastLogIndex();
        if (index < startLogIndex || index > lastLogIndex) {
            LOG.warn("index out of range, index={}, startLogIndex={}, lastLogIndex={}",
                    index, startLogIndex, lastLogIndex);
            return null;
        }
        Segment segment = startLogIndexSegmentMap.lowerEntry(index).getValue();
        return segment.getEntry(index);
    }

    public long getStartLogIndex() {
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
        return this.getEntry(lastLogIndex).getTerm();
    }

    public long append(List<Raft.LogEntry> entries) {
        long newLastLogIndex = this.getLastLogIndex();
        for (Raft.LogEntry entry : entries) {
            int entrySize = entry.getSerializedSize();
            int segmentSize = segments.size();
            boolean isNeedNewSegmentFile = false;
            try {
                if (segmentSize == 0) {
                    isNeedNewSegmentFile = true;
                } else {
                    Segment segment = segments.get(segmentSize - 1);
                    if (!segment.isCanWrite()) {
                        isNeedNewSegmentFile = true;
                    } else if (segment.getFileSize() + entrySize >= RaftOption.maxSegmentFileSize) {
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
                        segment.setRandomAccessFile(FileUtil.openFile(logDir, newFileName, "r"));
                    }
                }
                // 新建segment文件
                if (isNeedNewSegmentFile) {
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d", openedSegmentIndex.getAndIncrement());
                    File newSegmentFile = new File(newSegmentFileName);
                    newSegmentFile.createNewFile();
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(0);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(FileUtil.openFile(logDir, newSegmentFileName, "rw"));
                    segments.add(segment);
                }
                // 写proto到segment中
                if (entry.getIndex() == 0) {
                    newLastLogIndex++;
                    entry = Raft.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).build();
                } else {
                    newLastLogIndex = entry.getIndex();
                }
                segmentSize = segments.size();
                Segment segment = segments.get(segmentSize - 1);
                if (segment.getStartIndex() == 0) {
                    segment.setStartIndex(entry.getIndex());
                    startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
                }
                segment.setEndIndex(entry.getIndex());
                segment.getEntries().add(new Segment.Record(
                        segment.getRandomAccessFile().getFilePointer(), entry));
                FileUtil.writeProtoToFile(segment.getRandomAccessFile(), entry);
                segment.setFileSize(segment.getRandomAccessFile().length());
            }  catch (IOException ex) {
                throw new RuntimeException("meet exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }

    public void loadSegmentData(Segment segment) {
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0;
            while (offset < totalLength) {
                Raft.LogEntry entry = FileUtil.readProtoFromFile(randomAccessFile, Raft.LogEntry.class);
                Segment.Record record = new Segment.Record(offset, entry);
                segment.getEntries().add(record);
                offset = randomAccessFile.getFilePointer();
            }
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

    public List<Segment> readSegments() {
        List<String> fileNames = FileUtil.getSortedFilesInDirectory(logDir);
        for (String fileName: fileNames) {
            if (fileName.equals("metadata")) {
                continue;
            }
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
                    segment.setStartIndex(-1);
                    segment.setEndIndex(-1);
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
                segment.setRandomAccessFile(FileUtil.openFile(logDir, fileName, "r"));
                segment.setFileSize(segment.getRandomAccessFile().length());
                segments.add(segment);
            } catch (IOException ioException) {
                LOG.warn("open segment file error, file={}, msg={}",
                        fileName, ioException.getMessage());
                throw new RuntimeException("open segment file error");
            }
        }
        return segments;
    }


    public Raft.LogMetaData getMetaData() {
        return metaData;
    }

    public Raft.LogMetaData readMetaData() {
        String fileName = logDir + File.pathSeparator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            Raft.LogMetaData metadata = FileUtil.readProtoFromFile(randomAccessFile, Raft.LogMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    public void updateMetaData(Long currentTerm, Integer votedFor, Long startLogIndex) {
        Raft.LogMetaData.Builder builder = Raft.LogMetaData.newBuilder(this.metaData);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }
        if (startLogIndex != null) {
            builder.setStartLogIndex(startLogIndex);
        }
        this.metaData = builder.build();

        String fileName = logDir + File.pathSeparator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileUtil.writeProtoToFile(randomAccessFile, metaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

}
