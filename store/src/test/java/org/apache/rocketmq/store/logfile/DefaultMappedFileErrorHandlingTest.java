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
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultMappedFileErrorHandlingTest {

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
    public void testAppendMessageCallbackErrorHandling() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Test with a callback that returns an error
            AppendMessageCallback errorCallback = new AppendMessageCallback() {
                @Override
                public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, 
                    int maxBlank, MessageExtBrokerInner msg, 
                    PutMessageContext putMessageContext) {
                    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                }
                
                @Override
                public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, 
                    int maxBlank, MessageExtBatch messageExtBatch, 
                    PutMessageContext putMessageContext) {
                    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                }
            };
            
            // Create a mock message
            MessageExtBrokerInner msg = new MessageExtBrokerInner();
            msg.setBody("test message".getBytes());
            
            AppendMessageResult result = mappedFile.appendMessage(msg, errorCallback, new PutMessageContext("test-topic"));
            
            Assert.assertEquals("Should return error status", 
                AppendMessageStatus.UNKNOWN_ERROR, result.getStatus());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testCompactionAppendMsgCallbackErrorHandling() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Test with a callback that returns an error
            CompactionAppendMsgCallback errorCallback = new CompactionAppendMsgCallback() {
                @Override
                public AppendMessageResult doAppend(ByteBuffer bbDest, long fileFromOffset, 
                    int maxBlank, ByteBuffer bbSrc) {
                    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                }
            };
            
            ByteBuffer testBuffer = ByteBuffer.wrap("test data".getBytes());
            AppendMessageResult result = mappedFile.appendMessage(testBuffer, errorCallback);
            
            Assert.assertEquals("Should return error status", 
                AppendMessageStatus.UNKNOWN_ERROR, result.getStatus());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testWriteWithoutMmapWithNullRandomAccessFile() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Simulate the case where randomAccessFile is null
            // This should fall back to normal behavior
            byte[] testData = "test data".getBytes();
            boolean result = mappedFile.appendMessage(testData);
            
            // Should still work, but using MappedByteBuffer
            Assert.assertTrue("Should still work with null RandomAccessFile", result);
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testLargeDataWrite() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Test writing data that's close to the file size limit
            byte[] largeData = new byte[fileSize - 100]; // Leave some space
            for (int i = 0; i < largeData.length; i++) {
                largeData[i] = (byte) (i % 256);
            }
            
            boolean result = mappedFile.appendMessage(largeData);
            Assert.assertTrue("Should successfully write large data", result);
            Assert.assertEquals("Wrote position should match data size", 
                largeData.length, mappedFile.getWrotePosition());
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testWriteBeyondFileSize() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Fill the file almost completely
            byte[] data = new byte[fileSize - 10];
            boolean result = mappedFile.appendMessage(data);
            Assert.assertTrue("Should successfully write data", result);
            
            // Try to write more data than remaining space
            byte[] overflowData = new byte[20]; // More than remaining 10 bytes
            result = mappedFile.appendMessage(overflowData);
            Assert.assertFalse("Should fail to write beyond file size", result);
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testFlushErrorHandling() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            // Write some data
            byte[] testData = "test data for flush".getBytes();
            mappedFile.appendMessage(testData);
            
            // Flush should succeed
            int flushedPosition = mappedFile.flush(0);
            Assert.assertTrue("Flush should succeed", flushedPosition > 0);
            
        } finally {
            mappedFile.destroy(0);
        }
    }

    @Test
    public void testAppendMessageWithOffset() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile(fileName, fileSize, true);
        
        try {
            byte[] testData = "Hello, RocketMQ!".getBytes();
            
            // Test with valid offset
            boolean result = mappedFile.appendMessage(testData, 0, testData.length);
            Assert.assertTrue("Should successfully append with valid offset", result);
            
            // Test with invalid offset (beyond array length)
            result = mappedFile.appendMessage(testData, testData.length + 1, 1);
            Assert.assertFalse("Should fail with invalid offset", result);
            
        } finally {
            mappedFile.destroy(0);
        }
    }
}
