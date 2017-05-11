package com.github.wenweihu86.raft.util;

import com.google.protobuf.GeneratedMessageV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/6.
 */
public class RaftFileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RaftFileUtils.class);

    public static List<String> getSortedFilesInDirectory(String dirName) {
        File dir = new File(dirName);
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

        List<String> fileNames = new ArrayList<>(files.length);
        for (File file : files) {
            fileNames.add(file.getName());
        }
        return fileNames;
    }

    public static RandomAccessFile openFile(String dir, String fileName, String mode) {
        try {
            String fullFileName = dir + File.pathSeparator + fileName;
            File file = new File(fullFileName);
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException ex) {
            LOG.warn("file not fount, file={}", fileName);
            throw new RuntimeException("file not found, file={}" + fileName);
        }
    }

    public static <T extends GeneratedMessageV3> T readProtoFromFile(RandomAccessFile raf, Class<T> clazz) {
        try {
            int dataLen = raf.readInt();
            int hasReadLen = Integer.SIZE / Byte.SIZE;
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
            Method method = clazz.getMethod("parseFrom", byte[].class);
            T message = (T) method.invoke(clazz, data);
            return message;
        } catch (Exception ex) {
            LOG.warn("readProtoFromFile meet exception, {}", ex.getMessage());
            return null;
        }
    }

    public static  <T extends GeneratedMessageV3> void writeProtoToFile(RandomAccessFile raf, T message) {
        byte[] messageBytes = message.toByteArray();
        try {
            raf.writeInt(messageBytes.length);
            raf.write(messageBytes);
        } catch (IOException ex) {
            LOG.warn("write proto to file error, msg={}", ex.getMessage());
            throw new RuntimeException("write proto to file error");
        }
    }

}
