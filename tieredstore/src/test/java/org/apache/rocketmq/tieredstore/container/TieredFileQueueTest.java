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
package org.apache.rocketmq.tieredstore.container;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.mock.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredFileQueueTest {
    TieredMessageStoreConfig storeConfig;
    MessageQueue queue;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        queue = new MessageQueue("TieredFileQueueTest", storeConfig.getBrokerName(), 0);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID()));
        TieredStoreUtil.getMetadataStore(storeConfig).destroy();
    }

    @Test
    public void testGetFileSegment() throws ClassNotFoundException, NoSuchMethodException {
        TieredFileQueue fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.COMMIT_LOG,
            queue, storeConfig);
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
    public void testAppendAndRead() throws ClassNotFoundException, NoSuchMethodException {
        TieredFileQueue fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, storeConfig);
        fileQueue.setBaseOffset(0);
        Assert.assertEquals(0, fileQueue.getMinOffset());
        Assert.assertEquals(0, fileQueue.getCommitMsgQueueOffset());

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
    public void testLoadFromMetadata() throws ClassNotFoundException, NoSuchMethodException {
        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);

        MemoryFileSegment fileSegment1 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.COMMIT_LOG,
            queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize());
        fileSegment1.setFull();
        metadataStore.updateFileSegment(fileSegment1);
        metadataStore.updateFileSegment(fileSegment1);

        MemoryFileSegment fileSegment2 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.COMMIT_LOG,
            queue, 1100, storeConfig);
        metadataStore.updateFileSegment(fileSegment2);

        TieredFileQueue fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.COMMIT_LOG,
            queue, storeConfig);
        Assert.assertEquals(2, fileQueue.needCommitFileSegmentList.size());
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
    public void testCheckFileSize() throws ClassNotFoundException, NoSuchMethodException {
        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);

        TieredFileSegment fileSegment1 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize() - 100);
        fileSegment1.setFull(false);
        metadataStore.updateFileSegment(fileSegment1);
        metadataStore.updateFileSegment(fileSegment1);

        TieredFileSegment fileSegment2 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, 1100, storeConfig);
        fileSegment2.initPosition(fileSegment2.getSize() - 100);
        metadataStore.updateFileSegment(fileSegment2);
        metadataStore.updateFileSegment(fileSegment2);

        TieredFileQueue fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, storeConfig);
        Assert.assertEquals(1, fileQueue.needCommitFileSegmentList.size());

        fileSegment1 = fileQueue.getFileByIndex(0);
        Assert.assertTrue(fileSegment1.isFull());
        Assert.assertEquals(fileSegment1.getSize() + 100, fileSegment1.getCommitOffset());

        fileSegment2 = fileQueue.getFileByIndex(1);
        Assert.assertEquals(1000, fileSegment2.getCommitPosition());

        fileSegment2.setFull();
        fileQueue.commit(true);
        Assert.assertEquals(0, fileQueue.needCommitFileSegmentList.size());

        fileQueue.getFileToWrite();
        Assert.assertEquals(1, fileQueue.needCommitFileSegmentList.size());
    }

    @Test
    public void testCleanExpiredFile() throws ClassNotFoundException, NoSuchMethodException {
        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);

        TieredFileSegment fileSegment1 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize() - 100);
        fileSegment1.setFull(false);
        fileSegment1.setEndTimestamp(System.currentTimeMillis() - 1);
        metadataStore.updateFileSegment(fileSegment1);
        metadataStore.updateFileSegment(fileSegment1);

        long file1CreateTimeStamp = System.currentTimeMillis();

        TieredFileSegment fileSegment2 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, 1100, storeConfig);
        fileSegment2.initPosition(fileSegment2.getSize());
        fileSegment2.setEndTimestamp(System.currentTimeMillis() + 1);
        metadataStore.updateFileSegment(fileSegment2);
        metadataStore.updateFileSegment(fileSegment2);

        TieredFileQueue fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, storeConfig);
        Assert.assertEquals(2, fileQueue.getFileSegmentCount());

        fileQueue.cleanExpiredFile(file1CreateTimeStamp);
        fileQueue.destroyExpiredFile();
        Assert.assertEquals(1, fileQueue.getFileSegmentCount());
        Assert.assertNull(metadataStore.getFileSegment(fileSegment1));
        Assert.assertNotNull(metadataStore.getFileSegment(fileSegment2));

        fileQueue.cleanExpiredFile(Long.MAX_VALUE);
        fileQueue.destroyExpiredFile();
        Assert.assertEquals(0, fileQueue.getFileSegmentCount());
        Assert.assertNull(metadataStore.getFileSegment(fileSegment1));
        Assert.assertNull(metadataStore.getFileSegment(fileSegment2));
    }

    @Test
    public void testRollingNewFile() throws ClassNotFoundException, NoSuchMethodException {
        TieredMetadataStore metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);

        TieredFileSegment fileSegment1 = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, 100, storeConfig);
        fileSegment1.initPosition(fileSegment1.getSize() - 100);
        metadataStore.updateFileSegment(fileSegment1);

        TieredFileQueue fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.CONSUME_QUEUE,
            queue, storeConfig);
        Assert.assertEquals(1, fileQueue.getFileSegmentCount());

        fileQueue.rollingNewFile();
        Assert.assertEquals(2, fileQueue.getFileSegmentCount());
    }
}
