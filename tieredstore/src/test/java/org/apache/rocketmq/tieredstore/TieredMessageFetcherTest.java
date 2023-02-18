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
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.SelectMappedBufferResultWrapper;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.container.TieredContainerManager;
import org.apache.rocketmq.tieredstore.container.TieredIndexFile;
import org.apache.rocketmq.tieredstore.container.TieredMessageQueueContainer;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.mock.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtilTest;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TieredMessageFetcherTest {
    TieredMessageStoreConfig storeConfig;
    MessageQueue mq;
    TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        MemoryFileSegment.checkSize = false;
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        storeConfig.setReadAheadCacheExpireDuration(Long.MAX_VALUE);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(2);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(3);
        metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        mq = new MessageQueue("TieredMessageFetcherTest", storeConfig.getBrokerName(), 0);
    }

    @After
    public void tearDown() throws IOException {
        MemoryFileSegment.checkSize = true;
        FileUtils.deleteDirectory(new File(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID()));
        TieredStoreUtil.getMetadataStore(storeConfig).destroy();
        TieredContainerManager.getInstance(storeConfig).cleanup();
    }

    public Triple<TieredMessageFetcher, ByteBuffer, ByteBuffer> buildFetcher() {
        TieredContainerManager containerManager = TieredContainerManager.getInstance(storeConfig);
        TieredMessageFetcher fetcher = new TieredMessageFetcher(storeConfig);
        GetMessageResult getMessageResult = fetcher.getMessageAsync("group", mq.getTopic(), mq.getQueueId(), 0, 32, null).join();
        Assert.assertEquals(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, getMessageResult.getStatus());

        TieredMessageQueueContainer container = containerManager.getOrCreateMQContainer(mq);
        container.initOffset(0);

        getMessageResult = fetcher.getMessageAsync("group", mq.getTopic(), mq.getQueueId(), 0, 32, null).join();
        Assert.assertEquals(GetMessageStatus.NO_MESSAGE_IN_QUEUE, getMessageResult.getStatus());

        ByteBuffer msg1 = MessageBufferUtilTest.buildMessageBuffer();
        msg1.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 0);
        msg1.putLong(MessageBufferUtil.PHYSICAL_OFFSET_POSITION, 0);
        AppendResult result = container.appendCommitLog(msg1);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        ByteBuffer msg2 = MessageBufferUtilTest.buildMessageBuffer();
        msg2.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 1);
        msg2.putLong(MessageBufferUtil.PHYSICAL_OFFSET_POSITION, MessageBufferUtilTest.MSG_LEN);
        container.appendCommitLog(msg2);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        result = container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 0, 0, MessageBufferUtilTest.MSG_LEN, 0));
        Assert.assertEquals(AppendResult.SUCCESS, result);
        result = container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 1, MessageBufferUtilTest.MSG_LEN, MessageBufferUtilTest.MSG_LEN, 0));
        Assert.assertEquals(AppendResult.SUCCESS, result);

        container.commit(true);
        return Triple.of(fetcher, msg1, msg2);
    }

    @Test
    public void testGetMessageFromTieredStoreAsync() {
        Triple<TieredMessageFetcher, ByteBuffer, ByteBuffer> triple = buildFetcher();
        TieredMessageFetcher fetcher = triple.getLeft();
        ByteBuffer msg1 = triple.getMiddle();
        ByteBuffer msg2 = triple.getRight();
        TieredMessageQueueContainer container = TieredContainerManager.getInstance(storeConfig).getMQContainer(mq);
        Assert.assertNotNull(container);

        GetMessageResult getMessageResult = fetcher.getMessageFromTieredStoreAsync(container, 0, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(2, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(msg1, getMessageResult.getMessageBufferList().get(0));
        Assert.assertEquals(msg2, getMessageResult.getMessageBufferList().get(1));

        AppendResult result = container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 2, storeConfig.getReadAheadMessageSizeThreshold(), MessageBufferUtilTest.MSG_LEN, 0));
        Assert.assertEquals(AppendResult.SUCCESS, result);
        container.commit(true);
        getMessageResult = fetcher.getMessageFromTieredStoreAsync(container, 0, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(2, getMessageResult.getMessageBufferList().size());
    }

    @Test
    public void testGetMessageFromCacheAsync() {
        Triple<TieredMessageFetcher, ByteBuffer, ByteBuffer> triple = buildFetcher();
        TieredMessageFetcher fetcher = triple.getLeft();
        ByteBuffer msg1 = triple.getMiddle();
        ByteBuffer msg2 = triple.getRight();
        TieredMessageQueueContainer container = TieredContainerManager.getInstance(storeConfig).getMQContainer(mq);
        Assert.assertNotNull(container);

        fetcher.recordCacheAccess(container, "prevent-invalid-cache", 0, new ArrayList<>());
        Assert.assertEquals(0, fetcher.readAheadCache.estimatedSize());
        fetcher.putMessageToCache(container, 0, new SelectMappedBufferResult(0, msg1, msg1.remaining(), null), 0, 0, 1);
        Assert.assertEquals(1, fetcher.readAheadCache.estimatedSize());

        GetMessageResult getMessageResult = fetcher.getMessageFromCacheAsync(container, "group", 0, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(1, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(msg1, getMessageResult.getMessageBufferList().get(0));

        Awaitility.waitAtMost(3, TimeUnit.SECONDS)
            .until(() -> fetcher.readAheadCache.estimatedSize() == 2);
        ArrayList<SelectMappedBufferResultWrapper> wrapperList = new ArrayList<>();
        wrapperList.add(fetcher.getMessageFromCache(container, 0));
        fetcher.recordCacheAccess(container, "prevent-invalid-cache", 0, wrapperList);
        Assert.assertEquals(1, fetcher.readAheadCache.estimatedSize());
        wrapperList.clear();
        wrapperList.add(fetcher.getMessageFromCache(container, 1));
        fetcher.recordCacheAccess(container, "prevent-invalid-cache", 0, wrapperList);
        Assert.assertEquals(1, fetcher.readAheadCache.estimatedSize());

        SelectMappedBufferResult messageFromCache = fetcher.getMessageFromCache(container, 1).getDuplicateResult();
        fetcher.recordCacheAccess(container, "group", 0, wrapperList);
        Assert.assertNotNull(messageFromCache);
        Assert.assertEquals(msg2, messageFromCache.getByteBuffer());
        Assert.assertEquals(0, fetcher.readAheadCache.estimatedSize());
    }

    @Test
    public void testGetMessageAsync() {
        Triple<TieredMessageFetcher, ByteBuffer, ByteBuffer> triple = buildFetcher();
        TieredMessageFetcher fetcher = triple.getLeft();
        ByteBuffer msg1 = triple.getMiddle();
        ByteBuffer msg2 = triple.getRight();

        GetMessageResult getMessageResult = fetcher.getMessageAsync("group", mq.getTopic(), mq.getQueueId(), -1, 32, null).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_TOO_SMALL, getMessageResult.getStatus());

        getMessageResult = fetcher.getMessageAsync("group", mq.getTopic(), mq.getQueueId(), 2, 32, null).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_OVERFLOW_ONE, getMessageResult.getStatus());

        getMessageResult = fetcher.getMessageAsync("group", mq.getTopic(), mq.getQueueId(), 3, 32, null).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_OVERFLOW_BADLY, getMessageResult.getStatus());

        getMessageResult = fetcher.getMessageAsync("group", mq.getTopic(), mq.getQueueId(), 0, 32, null).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(2, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(msg1, getMessageResult.getMessageBufferList().get(0));
        Assert.assertEquals(msg2, getMessageResult.getMessageBufferList().get(1));
    }

    @Test
    public void testGetMessageStoreTimeStampAsync() {
        TieredMessageFetcher fetcher = new TieredMessageFetcher(storeConfig);
        TieredMessageQueueContainer container = TieredContainerManager.getInstance(storeConfig).getOrCreateMQContainer(mq);
        container.initOffset(0);

        ByteBuffer msg1 = MessageBufferUtilTest.buildMessageBuffer();
        msg1.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 0);
        msg1.putLong(MessageBufferUtil.PHYSICAL_OFFSET_POSITION, 0);
        long currentTimeMillis1 = System.currentTimeMillis();
        msg1.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, currentTimeMillis1);
        AppendResult result = container.appendCommitLog(msg1);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        ByteBuffer msg2 = MessageBufferUtilTest.buildMessageBuffer();
        msg2.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 1);
        msg2.putLong(MessageBufferUtil.PHYSICAL_OFFSET_POSITION, MessageBufferUtilTest.MSG_LEN);
        long currentTimeMillis2 = System.currentTimeMillis();
        msg2.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, currentTimeMillis2);
        container.appendCommitLog(msg2);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        result = container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 0, 0, MessageBufferUtilTest.MSG_LEN, 0));
        Assert.assertEquals(AppendResult.SUCCESS, result);
        result = container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 1, MessageBufferUtilTest.MSG_LEN, MessageBufferUtilTest.MSG_LEN, 0));
        Assert.assertEquals(AppendResult.SUCCESS, result);

        container.commit(true);

        long result1 = fetcher.getEarliestMessageTimeAsync(mq.getTopic(), mq.getQueueId()).join();
        long result2 = fetcher.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join();
        Assert.assertEquals(result1, result2);
        Assert.assertEquals(currentTimeMillis1, result1);

        long result3 = fetcher.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 1).join();
        Assert.assertEquals(currentTimeMillis2, result3);
    }

    @Test
    public void testGetOffsetInQueueByTime() {
        TieredMessageFetcher fetcher = new TieredMessageFetcher(storeConfig);
        Assert.assertEquals(-1, fetcher.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));

        TieredMessageQueueContainer container = TieredContainerManager.getInstance(storeConfig).getOrCreateMQContainer(mq);
        Assert.assertEquals(-1, fetcher.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));
        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 50, 0, MessageBufferUtilTest.MSG_LEN, 0), true);
        container.commit(true);
        Assert.assertEquals(-1, fetcher.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));


        long timestamp = System.currentTimeMillis();
        ByteBuffer buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 50);
        buffer.putLong(MessageBufferUtil.STORE_TIMESTAMP_POSITION, timestamp);
        container.initOffset(50);
        container.appendCommitLog(buffer, true);
        container.appendConsumeQueue(new DispatchRequest(mq.getTopic(), mq.getQueueId(), 0, MessageBufferUtilTest.MSG_LEN, 0, timestamp, 50, "", "", 0, 0, null), true);
        container.commit(true);
        Assert.assertEquals(50, fetcher.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));
    }

    @Test
    public void testQueryMessageAsync() {
        // skip this test on windows
        Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);

        TieredMessageFetcher fetcher = new TieredMessageFetcher(storeConfig);
        Assert.assertEquals(0, fetcher.queryMessageAsync(mq.getTopic(), "key", 32, 0, Long.MAX_VALUE).join().getMessageMapedList().size());

        TieredMessageQueueContainer container = TieredContainerManager.getInstance(storeConfig).getOrCreateMQContainer(mq);
        Assert.assertEquals(0, fetcher.queryMessageAsync(mq.getTopic(), "key", 32, 0, Long.MAX_VALUE).join().getMessageMapedList().size());

        container.initOffset(0);
        ByteBuffer buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 0);
        container.appendCommitLog(buffer);
        buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 1);
        container.appendCommitLog(buffer);
        buffer = MessageBufferUtilTest.buildMessageBuffer();
        buffer.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 2);
        container.appendCommitLog(buffer);

        DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), 0, MessageBufferUtilTest.MSG_LEN, 0, 0, 0, "", "key", 0, 0, null);
        container.appendIndexFile(request);
        request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN, MessageBufferUtilTest.MSG_LEN, 0, 0, 0, "", "key", 0, 0, null);
        container.appendIndexFile(request);
        request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), MessageBufferUtilTest.MSG_LEN * 2, MessageBufferUtilTest.MSG_LEN, 0, 0, 0, "", "another-key", 0, 0, null);
        container.appendIndexFile(request);
        container.commit(true);
        TieredIndexFile indexFile = TieredContainerManager.getIndexFile(storeConfig);
        indexFile.commit(true);
        Assert.assertEquals(1, fetcher.queryMessageAsync(mq.getTopic(), "key", 1, 0, Long.MAX_VALUE).join().getMessageMapedList().size());

        QueryMessageResult result = fetcher.queryMessageAsync(mq.getTopic(), "key", 32, 0, Long.MAX_VALUE).join();
        Assert.assertEquals(2, result.getMessageMapedList().size());
        Assert.assertEquals(1, result.getMessageMapedList().get(0).getByteBuffer().getLong(MessageBufferUtil.QUEUE_OFFSET_POSITION));
        Assert.assertEquals(0, result.getMessageMapedList().get(1).getByteBuffer().getLong(MessageBufferUtil.QUEUE_OFFSET_POSITION));
    }
}
