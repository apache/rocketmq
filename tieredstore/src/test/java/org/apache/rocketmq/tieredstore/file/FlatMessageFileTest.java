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
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlatMessageFileTest {

    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private MessageStoreConfig storeConfig;
    private MetadataStore metadataStore;
    private FlatFileFactory flatFileFactory;

    @Before
    public void init() throws ClassNotFoundException, NoSuchMethodException {
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        storeConfig.setCommitLogRollingInterval(0);
        storeConfig.setCommitLogRollingMinimumSize(999);
        metadataStore = new DefaultMetadataStore(storeConfig);
        flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);
    }

    @After
    public void shutdown() throws IOException {
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void testAppendCommitLog() {
        String topic = "CommitLogTest";
        FlatMessageFile flatFile = new FlatMessageFile(flatFileFactory, topic, 0);
        Assert.assertTrue(flatFile.getTopicId() >= 0);
        Assert.assertEquals(topic, flatFile.getMessageQueue().getTopic());
        Assert.assertEquals(0, flatFile.getMessageQueue().getQueueId());
        Assert.assertFalse(flatFile.isFlatFileInit());

        flatFile.flushMetadata();
        Assert.assertNotNull(metadataStore.getQueue(flatFile.getMessageQueue()));

        long offset = 100;
        flatFile.initOffset(offset);
        for (int i = 0; i < 5; i++) {
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            DispatchRequest request = new DispatchRequest(
                topic, 0, i, (long) buffer.remaining() * i, buffer.remaining(), 0L);
            flatFile.appendCommitLog(buffer);
            flatFile.appendConsumeQueue(request);
        }

        Assert.assertNotNull(flatFile.getFileLock());

        long time = MessageFormatUtil.getStoreTimeStamp(MessageFormatUtilTest.buildMockedMessageBuffer());
        Assert.assertEquals(time, flatFile.getMinStoreTimestamp());
        Assert.assertEquals(time, flatFile.getMaxStoreTimestamp());

        long size = MessageFormatUtilTest.buildMockedMessageBuffer().remaining();
        Assert.assertEquals(-1L, flatFile.getFirstMessageOffset());
        Assert.assertEquals(0L, flatFile.getCommitLogMinOffset());
        Assert.assertEquals(0L, flatFile.getCommitLogCommitOffset());
        Assert.assertEquals(5 * size, flatFile.getCommitLogMaxOffset());

        Assert.assertEquals(offset, flatFile.getConsumeQueueMinOffset());
        Assert.assertEquals(offset, flatFile.getConsumeQueueCommitOffset());
        Assert.assertEquals(offset + 5L, flatFile.getConsumeQueueMaxOffset());

        Assert.assertTrue(flatFile.commitAsync().join());
        Assert.assertEquals(6L, flatFile.getFirstMessageOffset());
        Assert.assertEquals(0L, flatFile.getCommitLogMinOffset());
        Assert.assertEquals(5 * size, flatFile.getCommitLogCommitOffset());
        Assert.assertEquals(5 * size, flatFile.getCommitLogMaxOffset());

        Assert.assertEquals(offset, flatFile.getConsumeQueueMinOffset());
        Assert.assertEquals(offset + 5L, flatFile.getConsumeQueueCommitOffset());
        Assert.assertEquals(offset + 5L, flatFile.getConsumeQueueMaxOffset());

        // test read
        ByteBuffer buffer = flatFile.getMessageAsync(offset).join();
        Assert.assertNotNull(buffer);
        Assert.assertEquals(size, buffer.remaining());
        Assert.assertEquals(6L, MessageFormatUtil.getQueueOffset(buffer));

        flatFile.destroyExpiredFile(0);
        flatFile.destroy();
    }

    @Test
    public void testEquals() {
        String topic = "EqualsTest";
        FlatMessageFile flatFile1 = new FlatMessageFile(flatFileFactory, topic, 0);
        FlatMessageFile flatFile2 = new FlatMessageFile(flatFileFactory, topic, 0);
        FlatMessageFile flatFile3 = new FlatMessageFile(flatFileFactory, topic, 1);
        Assert.assertEquals(flatFile1, flatFile2);
        Assert.assertEquals(flatFile1.hashCode(), flatFile2.hashCode());
        Assert.assertNotEquals(flatFile1, flatFile3);

        flatFile1.shutdown();
        flatFile2.shutdown();
        flatFile3.shutdown();

        flatFile1.destroy();
        flatFile2.destroy();
        flatFile3.destroy();
    }

    @Test
    public void testBinarySearchInQueueByTime() {

        // replace provider, need new factory again
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);

        // inject store time: 0, +100, +100, +100, +200
        MessageQueue mq = new MessageQueue("TopicTest", "BrokerName", 1);
        FlatMessageFile flatFile = new FlatMessageFile(flatFileFactory, MessageStoreUtil.toFilePath(mq));
        flatFile.initOffset(50);
        long timestamp1 = 1000;
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 50);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp1);
        flatFile.appendCommitLog(buffer);

        long timestamp2 = timestamp1 + 100;
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 51);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        flatFile.appendCommitLog(buffer);
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 52);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        flatFile.appendCommitLog(buffer);
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 53);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        flatFile.appendCommitLog(buffer);

        long timestamp3 = timestamp2 + 100;
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 54);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp3);
        flatFile.appendCommitLog(buffer);

        // append message to consume queue
        flatFile.consumeQueue.initOffset(50 * ConsumeQueue.CQ_STORE_UNIT_SIZE);

        for (int i = 0; i < 5; i++) {
            AppendResult appendResult = flatFile.appendConsumeQueue(new DispatchRequest(
                mq.getTopic(), mq.getQueueId(), MessageFormatUtilTest.MSG_LEN * i,
                MessageFormatUtilTest.MSG_LEN, 0, timestamp1, 50 + i,
                "", "", 0, 0, null));
            Assert.assertEquals(AppendResult.SUCCESS, appendResult);
        }

        // commit message will increase max consume queue offset
        Assert.assertTrue(flatFile.commitAsync().join());

        // offset:            50,  51,   52,   53,   54
        // inject store time: 0, +100, +100, +100, +200
        Assert.assertEquals(50, flatFile.getQueueOffsetByTimeAsync(0, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(50, flatFile.getQueueOffsetByTimeAsync(0, BoundaryType.UPPER).join().longValue());

        Assert.assertEquals(50, flatFile.getQueueOffsetByTimeAsync(timestamp1 - 1, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(50, flatFile.getQueueOffsetByTimeAsync(timestamp1 - 1, BoundaryType.UPPER).join().longValue());

        Assert.assertEquals(50, flatFile.getQueueOffsetByTimeAsync(timestamp1, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(50, flatFile.getQueueOffsetByTimeAsync(timestamp1, BoundaryType.UPPER).join().longValue());

        Assert.assertEquals(51, flatFile.getQueueOffsetByTimeAsync(timestamp1 + 1, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(51, flatFile.getQueueOffsetByTimeAsync(timestamp1 + 1, BoundaryType.UPPER).join().longValue());

        Assert.assertEquals(51, flatFile.getQueueOffsetByTimeAsync(timestamp2, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(53, flatFile.getQueueOffsetByTimeAsync(timestamp2, BoundaryType.UPPER).join().longValue());

        Assert.assertEquals(54, flatFile.getQueueOffsetByTimeAsync(timestamp2 + 1, BoundaryType.UPPER).join().longValue());
        Assert.assertEquals(54, flatFile.getQueueOffsetByTimeAsync(timestamp2 + 1, BoundaryType.LOWER).join().longValue());

        Assert.assertEquals(54, flatFile.getQueueOffsetByTimeAsync(timestamp3, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(54, flatFile.getQueueOffsetByTimeAsync(timestamp3, BoundaryType.UPPER).join().longValue());

        Assert.assertEquals(55, flatFile.getQueueOffsetByTimeAsync(timestamp3 + 1, BoundaryType.LOWER).join().longValue());
        Assert.assertEquals(55, flatFile.getQueueOffsetByTimeAsync(timestamp3 + 1, BoundaryType.UPPER).join().longValue());

        flatFile.destroy();
    }

    @Test
    public void testCommitLock() {
        String topic = "CommitLogTest";
        FlatMessageFile flatFile = new FlatMessageFile(flatFileFactory, topic, 0);
        flatFile.getCommitLock().drainPermits();
        Assert.assertFalse(flatFile.commitAsync().join());
    }
}
