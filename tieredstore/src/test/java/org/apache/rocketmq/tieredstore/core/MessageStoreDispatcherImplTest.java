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
package org.apache.rocketmq.tieredstore.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.file.FlatFileFactory;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.index.IndexItem;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.index.IndexStoreService;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class MessageStoreDispatcherImplTest {

    protected final String storePath = MessageStoreUtilTest.getRandomStorePath();
    protected MessageQueue mq;
    protected MetadataStore metadataStore;
    protected MessageStoreConfig storeConfig;
    protected MessageStoreExecutor executor;
    protected FlatFileStore fileStore;
    protected TieredMessageStore messageStore;

    @Before
    public void init() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredStoreFilePath(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        mq = new MessageQueue("StoreTest", storeConfig.getBrokerName(), 1);
        metadataStore = new DefaultMetadataStore(storeConfig);
        executor = new MessageStoreExecutor();
        fileStore = new FlatFileStore(messageStore, storeConfig, metadataStore, executor);
    }

    @After
    public void shutdown() throws IOException {
        if (messageStore != null) {
            messageStore.destroy();
        }
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void dispatchFromCommitLogTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(200L);
        Mockito.when(defaultStore.getMessageStoreConfig()).thenReturn(new org.apache.rocketmq.store.config.MessageStoreConfig());

        messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
        IndexService indexService =
                new IndexStoreService(messageStore, new FlatFileFactory(metadataStore, storeConfig), storePath);
        Mockito.when(messageStore.getIndexService()).thenReturn(indexService);

        // mock message
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        MessageExt messageExt = MessageDecoder.decode(buffer);
        messageExt.setKeys("Key");
        MessageAccessor.putProperty(
            messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "uk");
        messageExt.setBody(new byte[10]);
        messageExt.setStoreSize(0);
        buffer = ByteBuffer.wrap(MessageDecoder.encode(messageExt, false));
        buffer.putInt(0, buffer.remaining());

        DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(),
            MessageFormatUtil.getCommitLogOffset(buffer), buffer.remaining(), 0L,
            MessageFormatUtil.getStoreTimeStamp(buffer), 0L,
            "", "", 0, 0L, new HashMap<>());

        // construct flat file
        MessageStoreDispatcher dispatcher = new MessageStoreDispatcherImpl(messageStore);
        dispatcher.dispatch(request);
        FlatMessageFile flatFile = fileStore.getFlatFile(mq);
        Assert.assertNotNull(flatFile);

        // init offset
        dispatcher.doScheduleDispatch(flatFile, true).join();
        Assert.assertEquals(100L, flatFile.getConsumeQueueMinOffset());
        Assert.assertEquals(100L, flatFile.getConsumeQueueMaxOffset());
        Assert.assertEquals(100L, flatFile.getConsumeQueueCommitOffset());

        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);
        Mockito.when(cq.get(anyLong())).thenReturn(
            new CqUnit(100, 1000, buffer.remaining(), 0L));
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt())).thenReturn(
            new SelectMappedBufferResult(0L, buffer.asReadOnlyBuffer(), buffer.remaining(), null));
        dispatcher.doScheduleDispatch(flatFile, true).join();

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(() -> {
            List<IndexItem> resultList1 = indexService.queryAsync(
                mq.getTopic(), "uk", 32, 0L, System.currentTimeMillis()).join();
            List<IndexItem> resultList2 = indexService.queryAsync(
                mq.getTopic(), "uk", 120, 0L, System.currentTimeMillis()).join();
            Assert.assertEquals(32, resultList1.size());
            Assert.assertEquals(100, resultList2.size());
            return true;
        });

        Assert.assertEquals(100L, flatFile.getConsumeQueueMinOffset());
        Assert.assertEquals(200L, flatFile.getConsumeQueueMaxOffset());
        Assert.assertEquals(200L, flatFile.getConsumeQueueCommitOffset());
    }

    @Test
    public void dispatchServiceTest() {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
        IndexService indexService =
                new IndexStoreService(messageStore, new FlatFileFactory(metadataStore, storeConfig), storePath);
        Mockito.when(messageStore.getIndexService()).thenReturn(indexService);
        Mockito.when(defaultStore.getMessageStoreConfig()).thenReturn(new org.apache.rocketmq.store.config.MessageStoreConfig());

        // construct flat file
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(),
            MessageFormatUtil.getCommitLogOffset(buffer), buffer.remaining(), 0L,
            MessageFormatUtil.getStoreTimeStamp(buffer), 0L,
            "", "", 0, 0L, new HashMap<>());
        MessageStoreDispatcherImpl dispatcher = new MessageStoreDispatcherImpl(messageStore);
        dispatcher.dispatch(request);
        FlatMessageFile flatFile = fileStore.getFlatFile(mq);
        Assert.assertNotNull(flatFile);

        AtomicBoolean result = new AtomicBoolean(false);
        MessageStoreDispatcherImpl dispatcherSpy = Mockito.spy(dispatcher);
        Mockito.doAnswer(mock -> {
            result.set(true);
            return true;
        }).when(dispatcherSpy).dispatchWithSemaphore(any());
        dispatcherSpy.start();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(result::get);
        dispatcherSpy.shutdown();
    }
}
