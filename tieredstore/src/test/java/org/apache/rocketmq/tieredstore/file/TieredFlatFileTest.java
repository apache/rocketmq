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

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.memory.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TieredFlatFileTest {

    private final String storePath = TieredStoreTestUtil.getRandomStorePath();
    private MessageQueue queue;
    private TieredMessageStoreConfig storeConfig;
    private TieredFileAllocator fileQueueFactory;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.memory.MemoryFileSegment");
        queue = new MessageQueue("TieredFlatFileTest", storeConfig.getBrokerName(), 0);
        fileQueueFactory = new TieredFileAllocator(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    private List<FileSegmentMetadata> getSegmentMetadataList(TieredMetadataStore metadataStore) {
        List<FileSegmentMetadata> result = new ArrayList<>();
        metadataStore.iterateFileSegment(result::add);
        return result;
    }

    @Test
    public void testFileSegment() {
        MemoryFileSegment fileSegment = new MemoryFileSegment(
            FileSegmentType.COMMIT_LOG, queue, 100, storeConfig);
        fileSegment.initPosition(fileSegment.getSize());

        String filePath = TieredStoreUtil.toPath(queue);
        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForCommitLog(filePath);
        fileQueue.updateFileSegment(fileSegment);

        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        FileSegmentMetadata metadata =
            metadataStore.getFileSegment(filePath, FileSegmentType.COMMIT_LOG, 100);
        Assert.assertEquals(fileSegment.getPath(), metadata.getPath());
        Assert.assertEquals(FileSegmentType.COMMIT_LOG, FileSegmentType.valueOf(metadata.getType()));
        Assert.assertEquals(100, metadata.getBaseOffset());
        Assert.assertEquals(0, metadata.getSealTimestamp());

        fileSegment.setFull();
        fileQueue.updateFileSegment(fileSegment);
        metadata = metadataStore.getFileSegment(fileSegment.getPath(), FileSegmentType.COMMIT_LOG, 100);
        Assert.assertEquals(1000, metadata.getSize());
        Assert.assertEquals(0, metadata.getSealTimestamp());

        fileSegment.commit();
        fileQueue.updateFileSegment(fileSegment);
        metadata = metadataStore.getFileSegment(fileSegment.getPath(), FileSegmentType.COMMIT_LOG, 100);
        Assert.assertEquals(1000 + TieredCommitLog.CODA_SIZE, metadata.getSize());
        Assert.assertTrue(metadata.getSealTimestamp() > 0);

        MemoryFileSegment fileSegment2 = new MemoryFileSegment(FileSegmentType.COMMIT_LOG,
            queue, 1100, storeConfig);
        fileQueue.updateFileSegment(fileSegment2);
        List<FileSegmentMetadata> list = getSegmentMetadataList(metadataStore);
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(100, list.get(0).getBaseOffset());
        Assert.assertEquals(1100, list.get(1).getBaseOffset());

        Assert.assertNotNull(metadataStore.getFileSegment(
            fileSegment.getPath(), fileSegment.getFileType(), fileSegment.getBaseOffset()));
        metadataStore.deleteFileSegment(fileSegment.getPath(), fileSegment.getFileType());
        Assert.assertEquals(0L, getSegmentMetadataList(metadataStore).size());
    }

    /**
     * Test whether the file is continuous after switching to write.
     */
    @Test
    public void testGetFileSegment() {
        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForCommitLog(TieredStoreUtil.toPath(queue));
        fileQueue.setBaseOffset(0);
        TieredFileSegment segment1 = fileQueue.getFileToWrite();
        segment1.initPosition(1000);
        segment1.append(ByteBuffer.allocate(100), 0);
        segment1.setFull();
        segment1.commit();

        TieredFileSegment segment2 = fileQueue.getFileToWrite();
        Assert.assertNotSame(segment1, segment2);
        Assert.assertEquals(1000 + 100 + TieredCommitLog.CODA_SIZE, segment1.getMaxOffset());
        Assert.assertEquals(1000 + 100 + TieredCommitLog.CODA_SIZE, segment2.getBaseOffset());

        Assert.assertSame(fileQueue.getSegmentIndexByOffset(1000), 0);
        Assert.assertSame(fileQueue.getSegmentIndexByOffset(1050), 0);
        Assert.assertSame(fileQueue.getSegmentIndexByOffset(1100 + TieredCommitLog.CODA_SIZE), 1);
        Assert.assertSame(fileQueue.getSegmentIndexByOffset(1150), -1);
    }

    @Test
    public void testAppendAndRead() {
        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForConsumeQueue(TieredStoreUtil.toPath(queue));
        fileQueue.setBaseOffset(0);
        Assert.assertEquals(0, fileQueue.getMinOffset());
        Assert.assertEquals(0, fileQueue.getDispatchCommitOffset());

        TieredFileSegment segment1 = fileQueue.getFileToWrite();
        segment1.initPosition(segment1.getSize());
        Assert.assertEquals(0, segment1.getBaseOffset());
        Assert.assertEquals(1000, fileQueue.getCommitOffset());
        Assert.assertEquals(1000, fileQueue.getMaxOffset());

        ByteBuffer buffer = ByteBuffer.allocate(100);
        long currentTimeMillis = System.currentTimeMillis();
        buffer.putLong(currentTimeMillis);
        buffer.rewind();
        fileQueue.append(buffer);
        Assert.assertEquals(1100, segment1.getMaxOffset());

        segment1.setFull();
        fileQueue.commit(true);
        Assert.assertEquals(1100, segment1.getCommitOffset());

        ByteBuffer readBuffer = fileQueue.readAsync(1000, 8).join();
        Assert.assertEquals(currentTimeMillis, readBuffer.getLong());

        TieredFileSegment segment2 = fileQueue.getFileToWrite();
        Assert.assertNotEquals(segment1, segment2);
        segment2.initPosition(segment2.getSize());
        buffer.rewind();
        fileQueue.append(buffer);
        fileQueue.commit(true);
        readBuffer = fileQueue.readAsync(1000, 1200).join();
        Assert.assertEquals(currentTimeMillis, readBuffer.getLong(1100));
    }

    @Test
    public void testLoadFromMetadata() {
        String filePath = TieredStoreUtil.toPath(queue);
        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForCommitLog(filePath);

        MemoryFileSegment fileSegment1 =
            new MemoryFileSegment(FileSegmentType.COMMIT_LOG, queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize());
        fileSegment1.setFull();

        fileQueue.updateFileSegment(fileSegment1);
        fileQueue.updateFileSegment(fileSegment1);

        MemoryFileSegment fileSegment2 =
            new MemoryFileSegment(FileSegmentType.COMMIT_LOG, queue, 1100, storeConfig);
        fileQueue.updateFileSegment(fileSegment2);

        // Set instance to null and reload from disk
        TieredStoreUtil.metadataStoreInstance = null;
        fileQueue = fileQueueFactory.createFlatFileForCommitLog(filePath);
        Assert.assertEquals(2, fileQueue.getNeedCommitFileSegmentList().size());
        TieredFileSegment file1 = fileQueue.getFileByIndex(0);
        Assert.assertNotNull(file1);
        Assert.assertEquals(100, file1.getBaseOffset());
        Assert.assertFalse(file1.isFull());

        TieredFileSegment file2 = fileQueue.getFileByIndex(1);
        Assert.assertNotNull(file2);
        Assert.assertEquals(1100, file2.getBaseOffset());
        Assert.assertFalse(file2.isFull());

        TieredFileSegment file3 = fileQueue.getFileByIndex(2);
        Assert.assertNull(file3);
    }

    @Test
    public void testCheckFileSize() {
        String filePath = TieredStoreUtil.toPath(queue);
        TieredFlatFile tieredFlatFile = fileQueueFactory.createFlatFileForCommitLog(filePath);

        TieredFileSegment fileSegment1 = new MemoryFileSegment(
            FileSegmentType.CONSUME_QUEUE, queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize() - 100);
        fileSegment1.setFull();
        tieredFlatFile.updateFileSegment(fileSegment1);
        tieredFlatFile.updateFileSegment(fileSegment1);

        TieredFileSegment fileSegment2 = new MemoryFileSegment(
            FileSegmentType.CONSUME_QUEUE, queue, 1100, storeConfig);
        fileSegment2.initPosition(fileSegment2.getSize() - 100);
        tieredFlatFile.updateFileSegment(fileSegment2);
        tieredFlatFile.updateFileSegment(fileSegment2);

        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForConsumeQueue(filePath);
        Assert.assertEquals(1, fileQueue.getNeedCommitFileSegmentList().size());

        fileSegment1 = fileQueue.getFileByIndex(0);
        Assert.assertTrue(fileSegment1.isFull());
        Assert.assertEquals(fileSegment1.getSize() + 100, fileSegment1.getCommitOffset());

        fileSegment2 = fileQueue.getFileByIndex(1);
        Assert.assertEquals(1000, fileSegment2.getCommitPosition());

        fileSegment2.setFull();
        fileQueue.commit(true);
        Assert.assertEquals(0, fileQueue.getNeedCommitFileSegmentList().size());

        fileQueue.getFileToWrite();
        Assert.assertEquals(1, fileQueue.getNeedCommitFileSegmentList().size());
    }

    @Test
    public void testCleanExpiredFile() {
        String filePath = TieredStoreUtil.toPath(queue);
        TieredFlatFile tieredFlatFile = fileQueueFactory.createFlatFileForCommitLog(filePath);

        TieredFileSegment fileSegment1 = new MemoryFileSegment(
            FileSegmentType.CONSUME_QUEUE, queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize() - 100);
        fileSegment1.setFull(false);
        fileSegment1.setMaxTimestamp(System.currentTimeMillis() - 1);
        tieredFlatFile.updateFileSegment(fileSegment1);
        tieredFlatFile.updateFileSegment(fileSegment1);

        long file1CreateTimeStamp = System.currentTimeMillis();

        TieredFileSegment fileSegment2 = new MemoryFileSegment(
            FileSegmentType.CONSUME_QUEUE, queue, 1100, storeConfig);
        fileSegment2.initPosition(fileSegment2.getSize());
        fileSegment2.setMaxTimestamp(System.currentTimeMillis() + 1);
        tieredFlatFile.updateFileSegment(fileSegment2);
        tieredFlatFile.updateFileSegment(fileSegment2);

        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForConsumeQueue(filePath);
        Assert.assertEquals(2, fileQueue.getFileSegmentCount());

        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        fileQueue.cleanExpiredFile(file1CreateTimeStamp);
        fileQueue.destroyExpiredFile();
        Assert.assertEquals(1, fileQueue.getFileSegmentCount());
        Assert.assertNull(getMetadata(metadataStore, fileSegment1));
        Assert.assertNotNull(getMetadata(metadataStore, fileSegment2));

        fileQueue.cleanExpiredFile(Long.MAX_VALUE);
        fileQueue.destroyExpiredFile();
        Assert.assertEquals(0, fileQueue.getFileSegmentCount());
        Assert.assertNull(getMetadata(metadataStore, fileSegment1));
        Assert.assertNull(getMetadata(metadataStore, fileSegment2));
    }

    private FileSegmentMetadata getMetadata(TieredMetadataStore metadataStore, TieredFileSegment fileSegment) {
        return metadataStore.getFileSegment(
            fileSegment.getPath(), fileSegment.getFileType(), fileSegment.getBaseOffset());
    }

    @Test
    public void testRollingNewFile() {
        String filePath = TieredStoreUtil.toPath(queue);
        TieredFlatFile tieredFlatFile = fileQueueFactory.createFlatFileForCommitLog(filePath);

        TieredFileSegment fileSegment1 = new MemoryFileSegment(
            FileSegmentType.CONSUME_QUEUE, queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize() - 100);
        tieredFlatFile.updateFileSegment(fileSegment1);

        TieredFlatFile fileQueue = fileQueueFactory.createFlatFileForConsumeQueue(filePath);
        Assert.assertEquals(1, fileQueue.getFileSegmentCount());

        fileQueue.rollingNewFile();
        Assert.assertEquals(2, fileQueue.getFileSegmentCount());
    }

    @Test
    public void testGetFileByTime() {
        String filePath = TieredStoreUtil.toPath(queue);
        TieredFlatFile tieredFlatFile = fileQueueFactory.createFlatFileForCommitLog(filePath);
        TieredFileSegment fileSegment1 = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, queue, 1100, storeConfig);
        fileSegment1.setMinTimestamp(100);
        fileSegment1.setMaxTimestamp(200);

        TieredFileSegment fileSegment2 = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, queue, 1100, storeConfig);
        fileSegment2.setMinTimestamp(200);
        fileSegment2.setMaxTimestamp(300);

        tieredFlatFile.getFileSegmentList().add(fileSegment1);
        tieredFlatFile.getFileSegmentList().add(fileSegment2);

        TieredFileSegment segmentUpper = tieredFlatFile.getFileByTime(400, BoundaryType.UPPER);
        Assert.assertEquals(fileSegment2, segmentUpper);

        TieredFileSegment segmentLower = tieredFlatFile.getFileByTime(400, BoundaryType.LOWER);
        Assert.assertEquals(fileSegment2, segmentLower);


        TieredFileSegment segmentUpper2 = tieredFlatFile.getFileByTime(0, BoundaryType.UPPER);
        Assert.assertEquals(fileSegment1, segmentUpper2);

        TieredFileSegment segmentLower2 = tieredFlatFile.getFileByTime(0, BoundaryType.LOWER);
        Assert.assertEquals(fileSegment1, segmentLower2);


        TieredFileSegment segmentUpper3 = tieredFlatFile.getFileByTime(200, BoundaryType.UPPER);
        Assert.assertEquals(fileSegment1, segmentUpper3);

        TieredFileSegment segmentLower3 = tieredFlatFile.getFileByTime(200, BoundaryType.LOWER);
        Assert.assertEquals(fileSegment2, segmentLower3);
    }
}
