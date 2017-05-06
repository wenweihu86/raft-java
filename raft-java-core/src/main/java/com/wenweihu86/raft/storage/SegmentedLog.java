package com.wenweihu86.raft.storage;

import com.google.protobuf.*;
import com.wenweihu86.raft.RaftOption;
import com.wenweihu86.raft.proto.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

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

    public void append(List<Raft.LogEntry> entries) {
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
                        segment.setRandomAccessFile(this.openLogFile(newFileName, "r"));
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
                    segment.setRandomAccessFile(this.openLogFile(newSegmentFileName, "rw"));
                    segments.add(segment);
                }
                // 写proto到segment中
                segmentSize = segments.size();
                Segment segment = segments.get(segmentSize - 1);
                if (segment.getStartIndex() == 0) {
                    segment.setStartIndex(entry.getIndex());
                    startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
                }
                segment.setEndIndex(entry.getIndex());
                segment.getEntries().add(new Segment.Record(
                        segment.getRandomAccessFile().getFilePointer(), entry));
                writeProtoToFile(segment.getRandomAccessFile(), entry);
                segment.setFileSize(segment.getRandomAccessFile().length());
            }  catch (IOException ex) {
                throw new RuntimeException("meet exception, msg=" + ex.getMessage());
            }
        }
    }

    public void loadSegmentData(Segment segment) {
        try {
            RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0;
            while (offset < totalLength) {
                Raft.LogEntry entry = this.readProtoFromFile(randomAccessFile, Raft.LogEntry.class);
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
        File dir = new File(logDir);
        File[] files = dir.listFiles();
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                if (o1.isDirectory() && o2.isFile()) {
                    return -1;
                }
                if (o1.isFile() && o2.isDirectory()) {
                    return 1;
                }
                return o2.getName().compareTo(o1.getName());
            }
        });

        for (File file : files) {
            if (file.getName().equals("metadata1")
                    || file.getName().equals("metadata2")) {
                continue;
            }
            String fileName = file.getName();
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
                segment.setRandomAccessFile(this.openLogFile(fileName, "r"));
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
            Raft.LogMetaData metadata = this.readProtoFromFile(randomAccessFile, Raft.LogMetaData.class);
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
            this.writeProtoToFile(randomAccessFile, metaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

    public RandomAccessFile openLogFile(String fileName, String mode) {
        try {
            String fullFileName = this.logDir + File.pathSeparator + fileName;
            File file = new File(fullFileName);
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException ex) {
            LOG.warn("file not fount, file={}", fileName);
            throw new RuntimeException("file not found, file={}" + fileName);
        }
    }

    public <T extends GeneratedMessageV3> T readProtoFromFile(RandomAccessFile raf, Class<T> clazz) {
        try {
            long checksum = raf.readLong();
            int dataLen = raf.readInt();
            int hasReadLen = Long.SIZE / Byte.SIZE + Integer.SIZE / Byte.SIZE;

            if (raf.length() - hasReadLen < dataLen) {
                LOG.warn("file remainLength < dataLen");
                return null;
            }
            byte[] data = new byte[dataLen];
            int readLen = raf.read(data);
            if (readLen != dataLen) {
                LOG.warn("readLen != dataLen");
                return null;
            }

            CRC32 crc32Obj = new CRC32();
            crc32Obj.update(data);
            if (crc32Obj.getValue() != checksum) {
                LOG.warn("crc32 check failed");
                return null;
            }

            Method method = clazz.getMethod("parseFrom", byte[].class);
            T message = (T) method.invoke(clazz, data);
            return message;
        } catch (Exception ex) {
            LOG.warn("readProtoFromFile meet exception, {}", ex.getMessage());
            return null;
        }
    }

    public <T extends GeneratedMessageV3> void writeProtoToFile(RandomAccessFile raf, T message) {
        byte[] messageBytes = message.toByteArray();
        CRC32 crc32Obj = new CRC32();
        crc32Obj.update(messageBytes);
        long crc32 = crc32Obj.getValue();
        try {
            raf.writeLong(crc32);
            raf.writeInt(messageBytes.length);
            raf.write(messageBytes);
        } catch (IOException ex) {
            LOG.warn("write proto to file error, msg={}", ex.getMessage());
            throw new RuntimeException("write proto to file error");
        }
    }

}
