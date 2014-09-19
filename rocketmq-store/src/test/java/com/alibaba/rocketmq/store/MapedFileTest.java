/**
 * $Id: MapedFileTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class MapedFileTest {

    private static final String StoreMessage = "Once, there was a chance for me!";


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }


    @Test
    public void test_write_read() {
        try {
            MapedFile mapedFile = new MapedFile("./unit_test_store/MapedFileTest/000", 1024 * 64);
            boolean result = mapedFile.appendMessage(StoreMessage.getBytes());
            assertTrue(result);
            System.out.println("write OK");

            SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
            byte[] data = new byte[StoreMessage.length()];
            selectMapedBufferResult.getByteBuffer().get(data);
            String readString = new String(data);

            System.out.println("Read: " + readString);
            assertTrue(readString.equals(StoreMessage));

            // 禁止Buffer读写
            mapedFile.shutdown(1000);

            // mapedFile对象不可用
            assertTrue(!mapedFile.isAvailable());

            // 释放读到的Buffer
            selectMapedBufferResult.release();

            // 内存真正释放掉
            assertTrue(mapedFile.isCleanupOver());

            // 文件删除成功
            assertTrue(mapedFile.destroy(1000));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 当前测试用例由于对mmap操作错误，会导致JVM CRASHED
     */
    @Ignore
    public void test_jvm_crashed() {
        try {
            MapedFile mapedFile = new MapedFile("./unit_test_store/MapedFileTest/10086", 1024 * 64);
            boolean result = mapedFile.appendMessage(StoreMessage.getBytes());
            assertTrue(result);
            System.out.println("write OK");

            SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
            selectMapedBufferResult.release();
            mapedFile.shutdown(1000);

            byte[] data = new byte[StoreMessage.length()];
            selectMapedBufferResult.getByteBuffer().get(data);
            String readString = new String(data);
            System.out.println(readString);
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
