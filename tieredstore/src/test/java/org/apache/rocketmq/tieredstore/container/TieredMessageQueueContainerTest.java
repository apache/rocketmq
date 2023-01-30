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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.mock.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtilTest;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredMessageQueueContainerTest {
    TieredMessageStoreConfig storeConfig;
    MessageQueue mq;
    TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setCommitLogRollingInterval(0);
        storeConfig.setCommitLogRollingMinimumSize(999);
        mq = new MessageQueue("TieredMessageQueueContainerTest", storeConfig.getBrokerName(), 0);
        metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        MemoryFileSegment.checkSize = true;
        FileUtils.deleteDirectory(new File(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID()));
        TieredStoreUtil.getMetadataStore(storeConfig).destroy();
        TieredContainerManager.getInstance(storeConfig).cleanup();
    }

    @Test
    public void testAppendCommitLog() throws ClassNotFoundException, NoSuchMethodException, IOException {
        TieredMessageQueueContainer container = new TieredMessageQueueContainer(mq, storeConfig);
        ByteBuffer message = MessageBufferUtilTest.buildMessageBuffer();
        AppendResult result = container.appendCommitLog(message);
        Assert.assertEquals(AppendResult.OFFSET_INCORRECT, result);

        MemoryFileSegment segment = new MemoryFileSegment(TieredFileSegment.FileSegmentType.COMMIT_LOG, mq, 1000, storeConfig);
        segment.initPosition(segment.getSize());
        metadataStore.updateFileSegment(segment);
        metadataStore.updateFileSegment(segment);
        container = new TieredMessageQueueContainer(mq, storeConfig);
        container.initOffset(6);
        result = container.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        message.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 7);
        result = container.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        container.commit(true);
        Assert.assertEquals(7, container.getBuildCQMaxOffset());

        container.cleanExpiredFile(0);
        container.destroyExpiredFile();
        try {
            Field field = container.getClass().getDeclaredField("commitLog");
            field.setAccessible(true);
            TieredCommitLog commitLog = (TieredCommitLog) field.get(container);
            Field field2 = commitLog.getClass().getDeclaredField("fileQueue");
            field2.setAccessible(true);
            TieredFileQueue fileQueue = (TieredFileQueue) field2.get(commitLog);
            Assert.assertEquals(2, fileQueue.getFileSegmentCount());

            TieredFileSegment file1 = fileQueue.getFileByIndex(0);
            TieredFileSegment file2 = fileQueue.getFileByIndex(1);

            container.destroy();
            Assert.assertEquals(0, fileQueue.getFileSegmentCount());
            Assert.assertTrue(file1.isClosed());
            Assert.assertTrue(file2.isClosed());
        } catch (Exception e) {
            Assert.fail(e.getClass().getCanonicalName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testAppendConsumeQueue() throws ClassNotFoundException, NoSuchMethodException {
        TieredMessageQueueContainer container = new TieredMessageQueueContainer(mq, storeConfig);
        DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), 51, 2, 3, 4);
        AppendResult result = container.appendConsumeQueue(request);
        Assert.assertEquals(AppendResult.OFFSET_INCORRECT, result);

        MemoryFileSegment segment = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE, mq, 20, storeConfig);
        segment.initPosition(segment.getSize());
        metadataStore.updateFileSegment(segment);
        metadataStore.updateFileSegment(segment);
        container = new TieredMessageQueueContainer(mq, storeConfig);
        result = container.appendConsumeQueue(request);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), 52, 2, 3, 4);
        result = container.appendConsumeQueue(request);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        container.commit(true);
        container.flushMetadata();
        QueueMetadata queueMetadata = metadataStore.getQueue(mq);
        Assert.assertEquals(53, queueMetadata.getMaxOffset());
    }

    @Test
    public void testBinarySearchInQueueByTime() throws ClassNotFoundException, NoSuchMethodException {
        MemoryFileSegment.checkSize = false;

        TieredMessageQueueContainer container = new TieredMessageQueueContainer(mq, storeConfig);
        container.initOffset(50);
        long timestamp1 = System.currentTimeMillis();
        ByteBuffer buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 50);
        buffer.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, timestamp1);
        container.appendCommitLog(buffer, true);

        long timestamp2 = timestamp1 + 100;
        buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 51);
        buffer.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        container.appendCommitLog(buffer, true);
        buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 52);
        buffer.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        container.appendCommitLog(buffer, true);
        buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 53);
        buffer.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        container.appendCommitLog(buffer, true);

        long timestamp3 = timestamp2 + 100;
        buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 54);
        buffer.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, timestamp3);
        container.appendCommitLog(buffer, true);

        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 0, MessageBufferUtilTest.MSG_LEN, 0, timestamp1, 50, "", "", 0, 0, null), true);
        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN, MessageBufferUtilTest.MSG_LEN, 0, timestamp2, 51, "", "", 0, 0, null), true);
        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN * 2, MessageBufferUtilTest.MSG_LEN, 0, timestamp2, 52, "", "", 0, 0, null), true);
        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN * 3, MessageBufferUtilTest.MSG_LEN, 0, timestamp2, 53, "", "", 0, 0, null), true);
        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN * 4, MessageBufferUtilTest.MSG_LEN, 0, timestamp3, 54, "", "", 0, 0, null), true);
        container.commit(true);

        Assert.assertEquals(54, container.binarySearchInQueueByTime(timestamp3 + 1, BoundaryType.UPPER));
        Assert.assertEquals(54, container.binarySearchInQueueByTime(timestamp3, BoundaryType.UPPER));

        Assert.assertEquals(50, container.binarySearchInQueueByTime(timestamp1 - 1, BoundaryType.LOWER));
        Assert.assertEquals(50, container.binarySearchInQueueByTime(timestamp1, BoundaryType.LOWER));

        Assert.assertEquals(51, container.binarySearchInQueueByTime(timestamp1 + 1, BoundaryType.LOWER));
        Assert.assertEquals(51, container.binarySearchInQueueByTime(timestamp2, BoundaryType.LOWER));
        Assert.assertEquals(54, container.binarySearchInQueueByTime(timestamp2 + 1, BoundaryType.LOWER));
        Assert.assertEquals(54, container.binarySearchInQueueByTime(timestamp3, BoundaryType.LOWER));

        Assert.assertEquals(50, container.binarySearchInQueueByTime(timestamp1, BoundaryType.UPPER));
        Assert.assertEquals(50, container.binarySearchInQueueByTime(timestamp1 + 1, BoundaryType.UPPER));
        Assert.assertEquals(53, container.binarySearchInQueueByTime(timestamp2, BoundaryType.UPPER));
        Assert.assertEquals(53, container.binarySearchInQueueByTime(timestamp2 + 1, BoundaryType.UPPER));

        Assert.assertEquals(0, container.binarySearchInQueueByTime(timestamp1 - 1, BoundaryType.UPPER));
        Assert.assertEquals(55, container.binarySearchInQueueByTime(timestamp3 + 1, BoundaryType.LOWER));
    }
}
