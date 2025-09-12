/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.logfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultMappedFilePerformanceTest {

    private String storePath;
    private String fileName;
    private int fileSize = 10 * 1024 * 1024; // 10MB
    private static final int WRITE_COUNT = 10000;
    private static final int DATA_SIZE = 1024; // 1KB per write

    @Before
    public void setUp() throws Exception {
        storePath = System.getProperty("user.home") + File.separator + "unitteststore" + System.currentTimeMillis();
        fileName = storePath + File.separator + "00000000000000000000";
        UtilAll.ensureDirOK(storePath);
    }

    @After
    public void tearDown() throws Exception {
        UtilAll.deleteFile(new File(storePath));
    }

    @Test
    public void testWriteWithoutMmapPerformance() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            byte[] testData = new byte[DATA_SIZE];
            for (int i = 0; i < testData.length; i++) {
                testData[i] = (byte) (i % 256);
            }
            
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < WRITE_COUNT; i++) {
                boolean result = mappedFile.appendMessage(testData);
                Assert.assertTrue("Write should succeed", result);
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // WriteWithoutMmap Performance:
            //   Writes: WRITE_COUNT
            //   Data size per write: DATA_SIZE bytes
            //   Total data: (WRITE_COUNT * DATA_SIZE / 1024) KB
            //   Duration: duration ms
            //   Throughput: (WRITE_COUNT * DATA_SIZE / 1024.0 / duration * 1000) KB/s
            
            Assert.assertEquals("Wrote position should match expected", 
                WRITE_COUNT * DATA_SIZE, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testWriteWithMmapPerformance() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, false);
        
        try {
            byte[] testData = new byte[DATA_SIZE];
            for (int i = 0; i < testData.length; i++) {
                testData[i] = (byte) (i % 256);
            }
            
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < WRITE_COUNT; i++) {
                boolean result = mappedFile.appendMessage(testData);
                Assert.assertTrue("Write should succeed", result);
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // WriteWithMmap Performance:
            //   Writes: WRITE_COUNT
            //   Data size per write: DATA_SIZE bytes
            //   Total data: (WRITE_COUNT * DATA_SIZE / 1024) KB
            //   Duration: duration ms
            //   Throughput: (WRITE_COUNT * DATA_SIZE / 1024.0 / duration * 1000) KB/s
            
            Assert.assertEquals("Wrote position should match expected", 
                WRITE_COUNT * DATA_SIZE, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testFlushPerformance() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Write some data first
            byte[] testData = new byte[DATA_SIZE];
            for (int i = 0; i < testData.length; i++) {
                testData[i] = (byte) (i % 256);
            }
            
            for (int i = 0; i < 1000; i++) {
                mappedFile.appendMessage(testData);
            }
            
            long startTime = System.currentTimeMillis();
            
            int flushedPosition = mappedFile.flush(0);
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // Flush Performance:
            //   Flushed position: flushedPosition
            //   Duration: duration ms
            
            Assert.assertTrue("Flush should succeed", flushedPosition > 0);
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testByteBufferWritePerformance() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            ByteBuffer testBuffer = ByteBuffer.allocate(DATA_SIZE);
            for (int i = 0; i < DATA_SIZE; i++) {
                testBuffer.put((byte) (i % 256));
            }
            
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < WRITE_COUNT; i++) {
                testBuffer.rewind();
                boolean result = mappedFile.appendMessage(testBuffer);
                Assert.assertTrue("Write should succeed", result);
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // ByteBuffer Write Performance:
            //   Writes: WRITE_COUNT
            //   Data size per write: DATA_SIZE bytes
            //   Total data: (WRITE_COUNT * DATA_SIZE / 1024) KB
            //   Duration: duration ms
            //   Throughput: (WRITE_COUNT * DATA_SIZE / 1024.0 / duration * 1000) KB/s
            
            Assert.assertEquals("Wrote position should match expected", 
                WRITE_COUNT * DATA_SIZE, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testMixedWriteOperations() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            byte[] testData = new byte[DATA_SIZE];
            for (int i = 0; i < testData.length; i++) {
                testData[i] = (byte) (i % 256);
            }
            
            long startTime = System.currentTimeMillis();
            
            // Mix of different write operations
            for (int i = 0; i < WRITE_COUNT / 4; i++) {
                // appendMessage(byte[])
                boolean result1 = mappedFile.appendMessage(testData);
                Assert.assertTrue("Write should succeed", result1);
                
                // appendMessage(byte[], offset, length)
                boolean result2 = mappedFile.appendMessage(testData, 0, testData.length);
                Assert.assertTrue("Write should succeed", result2);
                
                // appendMessage(ByteBuffer)
                ByteBuffer buffer = ByteBuffer.wrap(testData);
                boolean result3 = mappedFile.appendMessage(buffer);
                Assert.assertTrue("Write should succeed", result3);
                
                // appendMessageUsingFileChannel(byte[])
                boolean result4 = mappedFile.appendMessageUsingFileChannel(testData);
                Assert.assertTrue("Write should succeed", result4);
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // Mixed Write Operations Performance:
            //   Total operations: WRITE_COUNT
            //   Data size per operation: DATA_SIZE bytes
            //   Total data: (WRITE_COUNT * DATA_SIZE / 1024) KB
            //   Duration: duration ms
            //   Throughput: (WRITE_COUNT * DATA_SIZE / 1024.0 / duration * 1000) KB/s
            
            Assert.assertEquals("Wrote position should match expected", 
                WRITE_COUNT * DATA_SIZE, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }
}
