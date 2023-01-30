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
package org.apache.rocketmq.tieredstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.container.TieredConsumeQueue;
import org.apache.rocketmq.tieredstore.container.TieredContainerManager;
import org.apache.rocketmq.tieredstore.container.TieredMessageQueueContainer;
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
import org.mockito.Mockito;

public class TieredDispatcherTest {
    TieredMessageStoreConfig storeConfig;
    MessageQueue mq;
    TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        MemoryFileSegment.checkSize = false;
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setBrokerName(storeConfig.getBrokerName());
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
    public void testDispatch() {
        metadataStore.addQueue(mq, 6);
        MemoryFileSegment segment = new MemoryFileSegment(TieredFileSegment.FileSegmentType.COMMIT_LOG, mq, 1000, storeConfig);
        segment.initPosition(segment.getSize());
        metadataStore.updateFileSegment(segment);
        metadataStore.updateFileSegment(segment);
        segment = new MemoryFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE, mq, 6 * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE, storeConfig);
        metadataStore.updateFileSegment(segment);

        TieredContainerManager containerManager = TieredContainerManager.getInstance(storeConfig);
        DefaultMessageStore defaultMessageStore = Mockito.mock(DefaultMessageStore.class);
        TieredDispatcher dispatcher = new TieredDispatcher(defaultMessageStore, storeConfig);

        SelectMappedBufferResult mockResult = new SelectMappedBufferResult(0, MessageBufferUtilTest.buildMessageBuffer(), MessageBufferUtilTest.MSG_LEN, null);
        Mockito.when(defaultMessageStore.selectOneMessageByOffset(7, MessageBufferUtilTest.MSG_LEN)).thenReturn(mockResult);
        DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), 6, 7, MessageBufferUtilTest.MSG_LEN, 1);
        dispatcher.dispatch(request);
        Assert.assertNotNull(containerManager.getMQContainer(mq));
        Assert.assertEquals(7, containerManager.getMQContainer(mq).getDispatchOffset());

        TieredMessageQueueContainer container = containerManager.getOrCreateMQContainer(mq);
        container.commit(true);
        Assert.assertEquals(6, container.getBuildCQMaxOffset());

        dispatcher.buildCQAndIndexFile();
        Assert.assertEquals(7, container.getConsumeQueueMaxOffset());

        ByteBuffer buffer1 = MessageBufferUtilTest.buildMessageBuffer();
        buffer1.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 7);
        container.appendCommitLog(buffer1);
        ByteBuffer buffer2 = MessageBufferUtilTest.buildMessageBuffer();
        buffer2.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 8);
        container.appendCommitLog(buffer2);
        ByteBuffer buffer3 = MessageBufferUtilTest.buildMessageBuffer();
        buffer3.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 9);
        container.appendCommitLog(buffer3);
        container.commitCommitLog();
        Assert.assertEquals(10, container.getDispatchOffset());

        dispatcher.handleAppendCommitLogResult(AppendResult.SUCCESS, container, 8, 8, 0, 0, 0, buffer1);
        dispatcher.handleAppendCommitLogResult(AppendResult.SUCCESS, container, 9, 9, 0, 0, 0, buffer2);
        dispatcher.buildCQAndIndexFile();
        Assert.assertEquals(7, container.getConsumeQueueMaxOffset());
        Assert.assertEquals(7, container.getDispatchOffset());


        dispatcher.handleAppendCommitLogResult(AppendResult.SUCCESS, container, 7, 7, 0, 0, 0, buffer1);
        dispatcher.handleAppendCommitLogResult(AppendResult.SUCCESS, container, 8, 8, 0, 0, 0, buffer2);
        dispatcher.handleAppendCommitLogResult(AppendResult.SUCCESS, container, 9, 9, 0, 0, 0, buffer3);
        dispatcher.buildCQAndIndexFile();
        Assert.assertEquals(10, container.getConsumeQueueMaxOffset());
    }

    @Test
    public void testDispatchByMQContainer() {
        metadataStore.addQueue(mq, 6);
        TieredContainerManager containerManager = TieredContainerManager.getInstance(storeConfig);
        DefaultMessageStore defaultStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(defaultStore.getConsumeQueue(mq.getTopic(), mq.getQueueId())).thenReturn(Mockito.mock(ConsumeQueue.class));
        TieredDispatcher dispatcher = new TieredDispatcher(defaultStore, storeConfig);

        Mockito.when(defaultStore.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId())).thenReturn(0L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(mq.getTopic(), mq.getQueueId())).thenReturn(9L);

        ByteBuffer cqItem = ByteBuffer.allocate(ConsumeQueue.CQ_STORE_UNIT_SIZE);
        cqItem.putLong(7);
        cqItem.putInt(MessageBufferUtilTest.MSG_LEN);
        cqItem.putLong(1);
        cqItem.flip();
        SelectMappedBufferResult mockResult = new SelectMappedBufferResult(0, cqItem, ConsumeQueue.CQ_STORE_UNIT_SIZE, null);
        Mockito.when(((ConsumeQueue) defaultStore.getConsumeQueue(mq.getTopic(), mq.getQueueId())).getIndexBuffer(6)).thenReturn(mockResult);

        cqItem = ByteBuffer.allocate(ConsumeQueue.CQ_STORE_UNIT_SIZE);
        cqItem.putLong(8);
        cqItem.putInt(MessageBufferUtilTest.MSG_LEN);
        cqItem.putLong(1);
        cqItem.flip();
        mockResult = new SelectMappedBufferResult(0, cqItem, ConsumeQueue.CQ_STORE_UNIT_SIZE, null);

        Mockito.when(((ConsumeQueue) defaultStore.getConsumeQueue(mq.getTopic(), mq.getQueueId())).getIndexBuffer(7)).thenReturn(mockResult);

        mockResult = new SelectMappedBufferResult(0, MessageBufferUtilTest.buildMessageBuffer(), MessageBufferUtilTest.MSG_LEN, null);
        Mockito.when(defaultStore.selectOneMessageByOffset(7, MessageBufferUtilTest.MSG_LEN)).thenReturn(mockResult);

        ByteBuffer msg = MessageBufferUtilTest.buildMessageBuffer();
        msg.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 7);
        mockResult = new SelectMappedBufferResult(0, msg, MessageBufferUtilTest.MSG_LEN, null);
        Mockito.when(defaultStore.selectOneMessageByOffset(8, MessageBufferUtilTest.MSG_LEN)).thenReturn(mockResult);

        dispatcher.dispatchByMQContainer(containerManager.getOrCreateMQContainer(mq));
        Assert.assertEquals(8, containerManager.getMQContainer(mq).getDispatchOffset());
    }
}
