package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.common.message.MessageDecoder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class Test1 {
    public static void main(String[] args) throws Exception {
//        File file = new File("D:\\20181205164735834");
//
//        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
//
//        FileChannel fileChannel = accessFile.getChannel();
//
//        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, file.length());
//
//        System.out.println(mappedByteBuffer.getLong(0));
//        System.out.println(mappedByteBuffer.getLong(8));
//        System.out.println(mappedByteBuffer.getLong(16));
//
//        mappedByteBuffer.putLong(8,1542680528840l) ;

        System.out.println(Integer.MIN_VALUE);

        int MULTI_TAGS_FLAG = 0x2 << 2;

        System.out.println(MULTI_TAGS_FLAG);


        System.out.println(0x1 << 0);

        Random random = new Random(System.currentTimeMillis());
        System.out.println(Math.abs(random.nextInt() % 99999999) % 1);
        System.out.println(Math.abs(random.nextInt() % 99999999) % 1);

        long physicalTotal = 1024 * 1024 * 1024 * 24L;
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
            physicalTotal = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
        }

        System.out.println("physicalTotal="+physicalTotal/(1024 * 1024 * 1024));
    }
}
