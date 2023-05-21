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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.mock.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtilTest;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompositeQueueFlatFileTest {

    private final String storePath = TieredStoreTestUtil.getRandomStorePath();
    private TieredMessageStoreConfig storeConfig;
    private TieredMetadataStore metadataStore;
    private TieredFileAllocator tieredFileAllocator;
    private MessageQueue mq;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setCommitLogRollingInterval(0);
        storeConfig.setCommitLogRollingMinimumSize(999);
        mq = new MessageQueue("CompositeQueueFlatFileTest", storeConfig.getBrokerName(), 0);
        metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        tieredFileAllocator = new TieredFileAllocator(storeConfig);
        TieredStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyContainerManager();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void testAppendCommitLog() {
        CompositeQueueFlatFile container = new CompositeQueueFlatFile(tieredFileAllocator, mq);
        ByteBuffer message = MessageBufferUtilTest.buildMessageBuffer();
        AppendResult result = container.appendCommitLog(message);
        Assert.assertEquals(AppendResult.OFFSET_INCORRECT, result);
        Assert.assertEquals(0L, container.commitLog.getFlatFile().getFileToWrite().getAppendPosition());
        Assert.assertEquals(0L, container.commitLog.getFlatFile().getFileToWrite().getCommitPosition());

        container = new CompositeQueueFlatFile(tieredFileAllocator, mq);
        container.initOffset(6);
        result = container.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        message.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 7);
        result = container.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        container.commit(true);
        Assert.assertEquals(7, container.getCommitLogDispatchCommitOffset());

        container.cleanExpiredFile(0);
        container.destroyExpiredFile();
    }

    @Test
    public void testAppendConsumeQueue() {
        CompositeQueueFlatFile container = new CompositeQueueFlatFile(tieredFileAllocator, mq);
        DispatchRequest request = new DispatchRequest(
            mq.getTopic(), mq.getQueueId(), 51, 2, 3, 4);
        AppendResult result = container.appendConsumeQueue(request);
        Assert.assertEquals(AppendResult.OFFSET_INCORRECT, result);

        // Create new segment in file queue
        MemoryFileSegment segment = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, mq, 20, storeConfig);
        segment.initPosition(segment.getSize());
        container.consumeQueue.getFlatFile().setBaseOffset(20L);
        container.consumeQueue.getFlatFile().getFileToWrite();

        // Recreate will load metadata and build consume queue
        container = new CompositeQueueFlatFile(tieredFileAllocator, mq);
        segment.initPosition(ConsumeQueue.CQ_STORE_UNIT_SIZE);
        result = container.appendConsumeQueue(request);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        request = new DispatchRequest(
            mq.getTopic(), mq.getQueueId(), 52, 2, 3, 4);
        result = container.appendConsumeQueue(request);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        container.commit(true);
        container.persistMetadata();

        QueueMetadata queueMetadata = metadataStore.getQueue(mq);
        Assert.assertEquals(53, queueMetadata.getMaxOffset());
    }

    @Test
    public void testBinarySearchInQueueByTime() throws ClassNotFoundException, NoSuchMethodException {

        // replace provider, need new factory again
        storeConfig.setTieredBackendServiceProvider(
            "org.apache.rocketmq.tieredstore.mock.MemoryFileSegmentWithoutCheck");
        tieredFileAllocator = new TieredFileAllocator(storeConfig);

        // inject store time: 0, +100, +100, +100, +200
        CompositeQueueFlatFile container = new CompositeQueueFlatFile(tieredFileAllocator, mq);
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

        // append message to consume queue
        container.consumeQueue.getFlatFile().setBaseOffset(50 * ConsumeQueue.CQ_STORE_UNIT_SIZE);

        for (int i = 0; i < 5; i++) {
            AppendResult appendResult = container.appendConsumeQueue(new DispatchRequest(
                mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN * i,
                MessageBufferUtilTest.MSG_LEN, 0, timestamp1, 50 + i,
                "", "", 0, 0, null), true);
            Assert.assertEquals(AppendResult.SUCCESS, appendResult);
        }

        // commit message will increase max consume queue offset
        container.commit(true);

        Assert.assertEquals(54, container.getOffsetInConsumeQueueByTime(timestamp3 + 1, BoundaryType.UPPER));
        Assert.assertEquals(54, container.getOffsetInConsumeQueueByTime(timestamp3, BoundaryType.UPPER));

        Assert.assertEquals(50, container.getOffsetInConsumeQueueByTime(timestamp1 - 1, BoundaryType.LOWER));
        Assert.assertEquals(50, container.getOffsetInConsumeQueueByTime(timestamp1, BoundaryType.LOWER));

        Assert.assertEquals(51, container.getOffsetInConsumeQueueByTime(timestamp1 + 1, BoundaryType.LOWER));
        Assert.assertEquals(51, container.getOffsetInConsumeQueueByTime(timestamp2, BoundaryType.LOWER));
        Assert.assertEquals(54, container.getOffsetInConsumeQueueByTime(timestamp2 + 1, BoundaryType.LOWER));
        Assert.assertEquals(54, container.getOffsetInConsumeQueueByTime(timestamp3, BoundaryType.LOWER));

        Assert.assertEquals(50, container.getOffsetInConsumeQueueByTime(timestamp1, BoundaryType.UPPER));
        Assert.assertEquals(50, container.getOffsetInConsumeQueueByTime(timestamp1 + 1, BoundaryType.UPPER));
        Assert.assertEquals(53, container.getOffsetInConsumeQueueByTime(timestamp2, BoundaryType.UPPER));
        Assert.assertEquals(53, container.getOffsetInConsumeQueueByTime(timestamp2 + 1, BoundaryType.UPPER));

        Assert.assertEquals(0, container.getOffsetInConsumeQueueByTime(timestamp1 - 1, BoundaryType.UPPER));
        Assert.assertEquals(55, container.getOffsetInConsumeQueueByTime(timestamp3 + 1, BoundaryType.LOWER));
    }
}
