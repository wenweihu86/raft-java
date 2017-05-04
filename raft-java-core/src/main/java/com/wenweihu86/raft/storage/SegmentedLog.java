package com.wenweihu86.raft.storage;

import com.google.protobuf.*;
import com.wenweihu86.raft.proto.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir;

    public List<Segment> readSegments() {
        File dir = new File(getClass().getResource(logDir).getFile());
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

        List<Segment> segments = new ArrayList<>();
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
            segment.setFileName(file.getName());
            segment.setFileSize(0);
            if (splitArray[0].equals("open")) {
                segment.setOpen(true);
                segment.setStartIndex(-1);
                segment.setEndIndex(-1);
                segments.add(segment);
            } else {
                try {
                    segment.setStartIndex(Long.parseLong(splitArray[0]));
                    segment.setEndIndex(Long.parseLong(splitArray[1]));
                    segment.setOpen(false);
                    segments.add(segment);
                } catch (NumberFormatException ex) {
                    LOG.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }
            }
        }
        return segments;
    }

    public Raft.Metadata readMetadata(String fileName) {
        String filePath = logDir + File.pathSeparator + fileName;
        try {
            File file = new File(getClass().getResource(filePath).getFile());
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            // TODO
        } catch (FileNotFoundException ex) {
            LOG.warn("meta file not exist, name={}", filePath);
            return null;
        }
        return null;
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

}
