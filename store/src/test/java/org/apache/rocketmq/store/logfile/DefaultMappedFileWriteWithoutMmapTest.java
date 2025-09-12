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
import org.apache.rocketmq.store.TransientStorePool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultMappedFileWriteWithoutMmapTest {

    private String storePath;
    private String fileName;
    private int fileSize = 1024 * 1024; // 1MB

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
    public void testWriteWithoutMmapEnabled() throws IOException {
        // Test with writeWithoutMmap = true
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Test appendMessage with byte array
            byte[] testData = "Hello, RocketMQ!".getBytes();
            boolean result = mappedFile.appendMessage(testData);
            Assert.assertTrue("Should successfully append message", result);
            Assert.assertEquals("Wrote position should be updated", testData.length, mappedFile.getWrotePosition());
            
            // Test appendMessage with ByteBuffer
            ByteBuffer buffer = ByteBuffer.wrap("Test ByteBuffer".getBytes());
            result = mappedFile.appendMessage(buffer);
            Assert.assertTrue("Should successfully append ByteBuffer", result);
            Assert.assertEquals("Wrote position should be updated", testData.length + "Test ByteBuffer".length(), mappedFile.getWrotePosition());
            
            // Test flush
            int flushedPosition = mappedFile.flush(0);
            Assert.assertTrue("Flush should succeed", flushedPosition > 0);
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testWriteWithoutMmapDisabled() throws IOException {
        // Test with writeWithoutMmap = false (default behavior)
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, false);
        
        try {
            // Test appendMessage with byte array
            byte[] testData = "Hello, RocketMQ!".getBytes();
            boolean result = mappedFile.appendMessage(testData);
            Assert.assertTrue("Should successfully append message", result);
            Assert.assertEquals("Wrote position should be updated", testData.length, mappedFile.getWrotePosition());
            
            // Test appendMessage with ByteBuffer
            ByteBuffer buffer = ByteBuffer.wrap("Test ByteBuffer".getBytes());
            result = mappedFile.appendMessage(buffer);
            Assert.assertTrue("Should successfully append ByteBuffer", result);
            Assert.assertEquals("Wrote position should be updated", testData.length + "Test ByteBuffer".length(), mappedFile.getWrotePosition());
            
            // Test flush
            int flushedPosition = mappedFile.flush(0);
            Assert.assertTrue("Flush should succeed", flushedPosition > 0);
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testWriteWithoutMmapWithTransientStorePool() throws IOException {
        // Test with writeWithoutMmap = true and TransientStorePool
        TransientStorePool transientStorePool = new TransientStorePool(5, fileSize);
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, transientStorePool, true);
        
        try {
            // Test appendMessage with byte array
            byte[] testData = "Hello, RocketMQ with TransientStorePool!".getBytes();
            boolean result = mappedFile.appendMessage(testData);
            Assert.assertTrue("Should successfully append message", result);
            Assert.assertEquals("Wrote position should be updated", testData.length, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testAppendMessageWithOffset() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            byte[] testData = "Hello, RocketMQ with offset!".getBytes();
            boolean result = mappedFile.appendMessage(testData, 0, testData.length);
            Assert.assertTrue("Should successfully append message with offset", result);
            Assert.assertEquals("Wrote position should be updated", testData.length, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testAppendMessageUsingFileChannel() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            byte[] testData = "Hello, RocketMQ using FileChannel!".getBytes();
            boolean result = mappedFile.appendMessageUsingFileChannel(testData);
            Assert.assertTrue("Should successfully append message using FileChannel", result);
            Assert.assertEquals("Wrote position should be updated", testData.length, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }
}
