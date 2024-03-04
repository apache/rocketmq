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
package org.apache.rocketmq.tieredstore.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlatAppendFileTest {

    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private MessageQueue queue;
    private MetadataStore metadataStore;
    private MessageStoreConfig storeConfig;
    private FlatFileFactory flatFileFactory;

    @Before
    public void init() throws ClassNotFoundException, NoSuchMethodException {
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredStoreFilePath(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        storeConfig.setTieredStoreCommitLogMaxSize(2000L);
        storeConfig.setTieredStoreConsumeQueueMaxSize(2000L);
        queue = new MessageQueue("TieredFlatFileTest", storeConfig.getBrokerName(), 0);
        metadataStore = new DefaultMetadataStore(storeConfig);
        flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);
    }

    @After
    public void shutdown() throws IOException {
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    public ByteBuffer allocateBuffer(int size) {
        byte[] byteArray = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        Arrays.fill(byteArray, (byte) 0);
        return buffer;
    }

    @Test
    public void recoverFileSizeTest() {
        String filePath = MessageStoreUtil.toFilePath(queue);
        FlatAppendFile flatFile = flatFileFactory.createFlatFileForConsumeQueue(filePath);
        flatFile.rollingNewFile(500L);

        FileSegment fileSegment = flatFile.getFileToWrite();
        flatFile.append(allocateBuffer(1000), 1L);
        flatFile.commitAsync().join();
        flatFile.flushFileSegmentMeta(fileSegment);
    }

    @Test
    public void testRecoverFile() {
        String filePath = MessageStoreUtil.toFilePath(queue);
        FlatAppendFile flatFile = flatFileFactory.createFlatFileForConsumeQueue(filePath);
        flatFile.rollingNewFile(500L);

        FileSegment fileSegment = flatFile.getFileToWrite();
        flatFile.append(allocateBuffer(1000), 1L);
        flatFile.commitAsync().join();
        flatFile.flushFileSegmentMeta(fileSegment);

        FileSegmentMetadata metadata =
            metadataStore.getFileSegment(filePath, FileSegmentType.CONSUME_QUEUE, 500L);
        Assert.assertEquals(fileSegment.getPath(), metadata.getPath());
        Assert.assertEquals(FileSegmentType.CONSUME_QUEUE, FileSegmentType.valueOf(metadata.getType()));
        Assert.assertEquals(500L, metadata.getBaseOffset());
        Assert.assertEquals(1000L, metadata.getSize());
        Assert.assertEquals(0L, metadata.getSealTimestamp());

        fileSegment.close();
        flatFile.rollingNewFile(flatFile.getAppendOffset());
        flatFile.append(allocateBuffer(200), 1L);
        flatFile.commitAsync().join();
        flatFile.flushFileSegmentMeta(fileSegment);
        Assert.assertEquals(2, flatFile.getFileSegmentList().size());

        metadata = metadataStore.getFileSegment(filePath, FileSegmentType.CONSUME_QUEUE, 1500L);
        Assert.assertEquals(fileSegment.getPath(), metadata.getPath());
        Assert.assertEquals(FileSegmentType.CONSUME_QUEUE, FileSegmentType.valueOf(metadata.getType()));
        Assert.assertEquals(1500L, metadata.getBaseOffset());
        Assert.assertEquals(200L, metadata.getSize());
        Assert.assertEquals(0L, metadata.getSealTimestamp());

        // reference same file
        flatFile = flatFileFactory.createFlatFileForConsumeQueue(filePath);
        Assert.assertEquals(2, flatFile.fileSegmentTable.size());
        metadata = metadataStore.getFileSegment(filePath, FileSegmentType.CONSUME_QUEUE, 1500L);
        Assert.assertEquals(fileSegment.getPath(), metadata.getPath());
        Assert.assertEquals(FileSegmentType.CONSUME_QUEUE, FileSegmentType.valueOf(metadata.getType()));
        Assert.assertEquals(1500L, metadata.getBaseOffset());
        Assert.assertEquals(200L, metadata.getSize());
        Assert.assertEquals(0L, metadata.getSealTimestamp());
        flatFile.destroy();
    }

    @Test
    public void testFileSegment() {
        String filePath = MessageStoreUtil.toFilePath(queue);
        FlatAppendFile flatFile = flatFileFactory.createFlatFileForConsumeQueue(filePath);
        Assert.assertThrows(IllegalStateException.class, flatFile::getFileToWrite);

        flatFile.commitAsync().join();
        flatFile.rollingNewFile(0L);
        Assert.assertEquals(0L, flatFile.getMinOffset());
        Assert.assertEquals(0L, flatFile.getCommitOffset());
        Assert.assertEquals(0L, flatFile.getAppendOffset());

        flatFile.append(allocateBuffer(1000), 1L);
        Assert.assertEquals(0L, flatFile.getMinOffset());
        Assert.assertEquals(0L, flatFile.getCommitOffset());
        Assert.assertEquals(1000L, flatFile.getAppendOffset());
        Assert.assertEquals(1L, flatFile.getMinTimestamp());
        Assert.assertEquals(1L, flatFile.getMaxTimestamp());

        flatFile.commitAsync().join();
        Assert.assertEquals(filePath, flatFile.getFilePath());
        Assert.assertEquals(FileSegmentType.CONSUME_QUEUE, flatFile.getFileType());
        Assert.assertEquals(0L, flatFile.getMinOffset());
        Assert.assertEquals(1000L, flatFile.getCommitOffset());
        Assert.assertEquals(1000L, flatFile.getAppendOffset());
        Assert.assertEquals(1L, flatFile.getMinTimestamp());
        Assert.assertEquals(1L, flatFile.getMaxTimestamp());

        // file full
        flatFile.append(allocateBuffer(1000), 1L);
        flatFile.append(allocateBuffer(1000), 1L);
        flatFile.commitAsync().join();
        Assert.assertEquals(2, flatFile.fileSegmentTable.size());
        flatFile.destroy();
    }

    @Test
    public void testAppendAndRead() {
        FlatAppendFile flatFile = flatFileFactory.createFlatFileForConsumeQueue(MessageStoreUtil.toFilePath(queue));
        flatFile.rollingNewFile(500L);
        Assert.assertEquals(500L, flatFile.getCommitOffset());
        Assert.assertEquals(500L, flatFile.getAppendOffset());

        flatFile.append(allocateBuffer(1000), 1L);

        // no commit
        CompletionException exception = Assert.assertThrows(
            CompletionException.class, () -> flatFile.readAsync(500, 200).join());
        Assert.assertTrue(exception.getCause() instanceof TieredStoreException);
        Assert.assertEquals(TieredStoreErrorCode.ILLEGAL_PARAM,
            ((TieredStoreException) exception.getCause()).getErrorCode());
        flatFile.commitAsync().join();
        Assert.assertEquals(200, flatFile.readAsync(500, 200).join().remaining());

        // 500-1500, 1500-3000
        flatFile.append(allocateBuffer(1500), 1L);
        flatFile.commitAsync().join();
        Assert.assertEquals(2, flatFile.fileSegmentTable.size());
        Assert.assertEquals(1000, flatFile.readAsync(1000, 1000).join().remaining());
        flatFile.destroy();
    }

    @Test
    public void testCleanExpiredFile() {
        FlatAppendFile flatFile = flatFileFactory.createFlatFileForConsumeQueue(MessageStoreUtil.toFilePath(queue));
        flatFile.destroyExpiredFile(1);

        flatFile.rollingNewFile(500L);
        flatFile.append(allocateBuffer(1000), 2L);
        flatFile.commitAsync().join();
        Assert.assertEquals(1, flatFile.fileSegmentTable.size());
        flatFile.destroyExpiredFile(1);
        Assert.assertEquals(1, flatFile.fileSegmentTable.size());
        flatFile.destroyExpiredFile(3);
        Assert.assertEquals(0, flatFile.fileSegmentTable.size());

        flatFile.rollingNewFile(1500L);
        flatFile.append(allocateBuffer(1000), 2L);
        flatFile.append(allocateBuffer(1000), 2L);
        flatFile.commitAsync().join();
        flatFile.destroy();
        Assert.assertEquals(0, flatFile.fileSegmentTable.size());
    }

//    @Test
//    public void testGetFileByTime() {
//        String filePath = TieredStoreUtil.toPath(queue);
//        FlatCompositeFile flatCompositeFile = fileQueueFactory.createFlatFileForCommitLog(filePath);
//        FileSegment fileSegment1 = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, queue, 1100, storeConfig);
//        fileSegment1.setMinTimestamp(100);
//        fileSegment1.setMaxTimestamp(200);
//
//        FileSegment fileSegment2 = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, queue, 1100, storeConfig);
//        fileSegment2.setMinTimestamp(200);
//        fileSegment2.setMaxTimestamp(300);
//
//        flatCompositeFile.getFileSegmentList().add(fileSegment1);
//        flatCompositeFile.getFileSegmentList().add(fileSegment2);
//
//        FileSegment segmentUpper = flatCompositeFile.getFileByTime(400, BoundaryType.UPPER);
//        Assert.assertEquals(fileSegment2, segmentUpper);
//
//        FileSegment segmentLower = flatCompositeFile.getFileByTime(400, BoundaryType.LOWER);
//        Assert.assertEquals(fileSegment2, segmentLower);
//
//
//        FileSegment segmentUpper2 = flatCompositeFile.getFileByTime(0, BoundaryType.UPPER);
//        Assert.assertEquals(fileSegment1, segmentUpper2);
//
//        FileSegment segmentLower2 = flatCompositeFile.getFileByTime(0, BoundaryType.LOWER);
//        Assert.assertEquals(fileSegment1, segmentLower2);
//
//
//        FileSegment segmentUpper3 = flatCompositeFile.getFileByTime(200, BoundaryType.UPPER);
//        Assert.assertEquals(fileSegment1, segmentUpper3);
//
//        FileSegment segmentLower3 = flatCompositeFile.getFileByTime(200, BoundaryType.LOWER);
//        Assert.assertEquals(fileSegment2, segmentLower3);
//    }
}
