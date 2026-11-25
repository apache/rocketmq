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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.GroupCommitContext;
import org.apache.rocketmq.tieredstore.file.FlatFileFactory;
import org.apache.rocketmq.tieredstore.file.FlatFileInterface;
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
        fileStore = new FlatFileStore(storeConfig, metadataStore, executor);
    }

    @After
    public void shutdown() throws IOException {
        if (messageStore != null) {
            messageStore.destroy();
        }
        executor.shutdown();
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void dispatchFromCommitLogTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(200L);

        messageStore = Mockito.mock(TieredMessageStore.class);
        IndexService indexService =
            new IndexStoreService(new FlatFileFactory(metadataStore, storeConfig, executor), storePath);
        indexService.start();
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
        Mockito.when(messageStore.getIndexService()).thenReturn(indexService);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(new org.apache.rocketmq.store.config.MessageStoreConfig());

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

        // Index construction is submitted to the buffer commit executor asynchronously,
        // so it may not be visible right after doScheduleDispatch returns. Use untilAsserted
        // here, only it retries when the assertion inside fails.
        Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(100))
            .atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                List<IndexItem> resultList1 = indexService.queryAsync(
                    mq.getTopic(), "uk", 32, 0L, System.currentTimeMillis()).join();
                List<IndexItem> resultList2 = indexService.queryAsync(
                    mq.getTopic(), "uk", 120, 0L, System.currentTimeMillis()).join();
                Assert.assertEquals(32, resultList1.size());
                Assert.assertEquals(100, resultList2.size());
            });

        Assert.assertEquals(100L, flatFile.getConsumeQueueMinOffset());
        Assert.assertEquals(200L, flatFile.getConsumeQueueMaxOffset());
        Assert.assertEquals(200L, flatFile.getConsumeQueueCommitOffset());
    }

    @Test
    public void dispatchCommitFailedTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(200L);

        messageStore = Mockito.mock(TieredMessageStore.class);
        IndexService indexService =
            new IndexStoreService(new FlatFileFactory(metadataStore, storeConfig, executor), storePath);
        indexService.start();
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
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
        flatFile.getCommitLock().drainPermits();
        dispatcher.doScheduleDispatch(flatFile, true).join();
        GroupCommitContext groupCommitContext = ((MessageStoreDispatcherImpl)dispatcher).getFailedGroupCommitMap().get(flatFile);
        Assert.assertTrue(groupCommitContext != null);
        Assert.assertTrue(groupCommitContext.getEndOffset() == 200);
        flatFile.getCommitLock().release();
        flatFile.commitAsync().join();
        dispatcher.doScheduleDispatch(flatFile, true).join();
        Assert.assertTrue(((MessageStoreDispatcherImpl)dispatcher).getFailedGroupCommitMap().get(flatFile) == null);
        ((MessageStoreDispatcherImpl)dispatcher).flatFileStore.destroyFile(mq);
        ((MessageStoreDispatcherImpl)dispatcher).releaseClosedPendingGroupCommit();

    }

    @Test
    public void dispatchFailedGroupCommitMapReleaseTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(200L);

        messageStore = Mockito.mock(TieredMessageStore.class);
        IndexService indexService =
            new IndexStoreService(new FlatFileFactory(metadataStore, storeConfig, executor), storePath);
        indexService.start();
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
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
        flatFile.getCommitLock().drainPermits();
        dispatcher.doScheduleDispatch(flatFile, true).join();
        GroupCommitContext groupCommitContext = ((MessageStoreDispatcherImpl)dispatcher).getFailedGroupCommitMap().get(flatFile);
        Assert.assertTrue(groupCommitContext != null);
        ((MessageStoreDispatcherImpl)dispatcher).flatFileStore.destroyFile(mq);
        ((MessageStoreDispatcherImpl)dispatcher).releaseClosedPendingGroupCommit();
        Assert.assertTrue(((MessageStoreDispatcherImpl)dispatcher).getFailedGroupCommitMap().get(flatFile) == null);

    }

    private MessageStoreDispatcherImpl buildMockedDispatcher(MessageStore defaultStore, IndexService indexService) {
        messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
        Mockito.when(messageStore.getIndexService()).thenReturn(indexService);
        return new MessageStoreDispatcherImpl(messageStore);
    }

    private FlatFileInterface mockInitializedFlatFile() {
        FlatFileInterface flatFile = Mockito.mock(FlatFileInterface.class);
        Mockito.when(flatFile.getMessageQueue()).thenReturn(mq);
        Mockito.when(flatFile.isFlatFileInit()).thenReturn(true);
        Mockito.when(flatFile.getConsumeQueueMaxOffset()).thenReturn(100L);
        Mockito.when(flatFile.getConsumeQueueCommitOffset()).thenReturn(100L);
        Mockito.when(flatFile.getFileLock()).thenReturn(new ReentrantLock());
        Mockito.when(flatFile.rollingFile(Mockito.anyLong())).thenReturn(false);
        Mockito.when(flatFile.getTopicId()).thenReturn(0L);
        Mockito.when(flatFile.getCommitLogMaxOffset()).thenReturn(1000L);
        Mockito.when(flatFile.commitAsync()).thenReturn(CompletableFuture.completedFuture(true));
        return flatFile;
    }

    private static ByteBuffer buildFreshMockedMessageBuffer() {
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        // Keep the batch from looking timed out, so a partially built batch is not re-dispatched
        // immediately and the test observes exactly one append round.
        buffer.putLong(buffer.position() + MessageFormatUtil.STORE_TIMESTAMP_POSITION,
            System.currentTimeMillis());
        return buffer;
    }

    private static SelectMappedBufferResult newMockedBuffer(ByteBuffer buffer) {
        return new SelectMappedBufferResult(0L, buffer.asReadOnlyBuffer(), buffer.remaining(),
            Mockito.mock(MappedFile.class));
    }

    @Test
    public void dispatchMidBatchConsumeQueueTruncationTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(102L);

        FlatFileInterface flatFile = mockInitializedFlatFile();
        Mockito.when(flatFile.appendCommitLog(any(SelectMappedBufferResult.class))).thenReturn(AppendResult.SUCCESS);
        Mockito.when(flatFile.appendConsumeQueue(any(DispatchRequest.class))).thenReturn(AppendResult.SUCCESS);
        MessageStoreDispatcherImpl dispatcher =
            buildMockedDispatcher(defaultStore, Mockito.mock(IndexService.class));

        ByteBuffer buffer = buildFreshMockedMessageBuffer();
        SelectMappedBufferResult preflight = newMockedBuffer(buffer);
        SelectMappedBufferResult appended = newMockedBuffer(buffer);

        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);
        // The preflight offset is still readable, the next one has been truncated away by the
        // expiry sweep while the batch was being built.
        Mockito.when(cq.get(anyLong())).thenReturn(null);
        Mockito.when(cq.get(100L)).thenReturn(new CqUnit(100, 1000, buffer.remaining(), 0L));
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt()))
            .thenReturn(preflight, appended);

        // Must complete normally instead of throwing NullPointerException from cqUnit.getSize().
        dispatcher.doScheduleDispatch(flatFile, true).join();

        Mockito.verify(flatFile, Mockito.times(1)).appendCommitLog(any(SelectMappedBufferResult.class));
        Mockito.verify(flatFile, Mockito.times(1)).appendConsumeQueue(any(DispatchRequest.class));
        Mockito.verify(flatFile, Mockito.times(1)).commitAsync();
        Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50))
            .atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Assert.assertTrue("preflight buffer was not released", preflight.hasReleased());
                Assert.assertTrue("buffer of the committed partial batch was not released", appended.hasReleased());
            });
    }

    @Test
    public void dispatchMidBatchCommitLogExpiredTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(102L);

        FlatFileInterface flatFile = mockInitializedFlatFile();
        Mockito.when(flatFile.appendCommitLog(any(SelectMappedBufferResult.class))).thenReturn(AppendResult.SUCCESS);
        Mockito.when(flatFile.appendConsumeQueue(any(DispatchRequest.class))).thenReturn(AppendResult.SUCCESS);
        MessageStoreDispatcherImpl dispatcher =
            buildMockedDispatcher(defaultStore, Mockito.mock(IndexService.class));

        ByteBuffer buffer = buildFreshMockedMessageBuffer();
        SelectMappedBufferResult preflight = newMockedBuffer(buffer);
        SelectMappedBufferResult appended = newMockedBuffer(buffer);

        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);
        Mockito.when(cq.get(anyLong())).thenReturn(new CqUnit(100, 1000, buffer.remaining(), 0L));
        // The first commitlog read succeeds, the next one has expired and is gone.
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt()))
            .thenReturn(preflight, appended, null);

        dispatcher.doScheduleDispatch(flatFile, true).join();

        Mockito.verify(flatFile, Mockito.times(1)).appendCommitLog(any(SelectMappedBufferResult.class));
        Mockito.verify(flatFile, Mockito.times(1)).appendConsumeQueue(any(DispatchRequest.class));
        Mockito.verify(flatFile, Mockito.times(1)).commitAsync();
        Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50))
            .atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Assert.assertTrue("preflight buffer was not released", preflight.hasReleased());
                Assert.assertTrue("buffer of the committed partial batch was not released", appended.hasReleased());
            });
    }

    @Test
    public void dispatchConsumeQueueAppendFailureHoldsBuffersUntilCommitTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(101L);

        FlatFileInterface flatFile = mockInitializedFlatFile();
        CompletableFuture<Boolean> commitFuture = new CompletableFuture<>();
        Mockito.when(flatFile.commitAsync()).thenReturn(commitFuture);
        // The commitlog append succeeds, but the very first consume queue append fails.
        Mockito.when(flatFile.appendCommitLog(any(SelectMappedBufferResult.class))).thenReturn(AppendResult.SUCCESS);
        Mockito.when(flatFile.appendConsumeQueue(any(DispatchRequest.class))).thenReturn(AppendResult.UNKNOWN_ERROR);
        MessageStoreDispatcherImpl dispatcher =
            buildMockedDispatcher(defaultStore, Mockito.mock(IndexService.class));

        ByteBuffer buffer = buildFreshMockedMessageBuffer();
        SelectMappedBufferResult preflight = newMockedBuffer(buffer);
        SelectMappedBufferResult loopResult = newMockedBuffer(buffer);

        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);
        Mockito.when(cq.get(anyLong())).thenReturn(new CqUnit(100, 1000, buffer.remaining(), 0L));
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt()))
            .thenReturn(preflight, loopResult);

        dispatcher.doScheduleDispatch(flatFile, true).join();

        // The commitlog append already holds the mapped bytes, so the buffer must stay alive
        // until the upload that reads it has finished.
        Mockito.verify(flatFile, Mockito.times(1)).commitAsync();
        Assert.assertFalse("buffer was released before its commit finished", loopResult.hasReleased());

        commitFuture.complete(true);
        Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50))
            .atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Assert.assertTrue("preflight buffer was not released", preflight.hasReleased());
                Assert.assertTrue("buffer retained by the commitlog was not released after commit", loopResult.hasReleased());
            });
    }

    @Test
    public void dispatchConsumeQueueAppendFailureKeepsContextWhenCommitBusyTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(101L);

        FlatFileInterface flatFile = mockInitializedFlatFile();
        // The commit lock is held by an in-flight commit, so this round cannot start a commit.
        Mockito.when(flatFile.commitAsync()).thenReturn(CompletableFuture.completedFuture(false));
        Mockito.when(flatFile.appendCommitLog(any(SelectMappedBufferResult.class))).thenReturn(AppendResult.SUCCESS);
        Mockito.when(flatFile.appendConsumeQueue(any(DispatchRequest.class))).thenReturn(AppendResult.UNKNOWN_ERROR);
        MessageStoreDispatcherImpl dispatcher =
            buildMockedDispatcher(defaultStore, Mockito.mock(IndexService.class));

        ByteBuffer buffer = buildFreshMockedMessageBuffer();
        SelectMappedBufferResult preflight = newMockedBuffer(buffer);
        SelectMappedBufferResult loopResult = newMockedBuffer(buffer);

        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);
        Mockito.when(cq.get(anyLong())).thenReturn(new CqUnit(100, 1000, buffer.remaining(), 0L));
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt()))
            .thenReturn(preflight, loopResult);

        dispatcher.doScheduleDispatch(flatFile, true).join();

        GroupCommitContext failedContext = dispatcher.getFailedGroupCommitMap().get(flatFile);
        Assert.assertNotNull("the pending group commit context was dropped", failedContext);
        Assert.assertEquals(101L, failedContext.getEndOffset());
        Assert.assertFalse("buffer was released before its commit finished", loopResult.hasReleased());

        // Simulate the next round: the missing consume queue entry was retried and committed,
        // so the consume queue commit offset has moved past the failed offset.
        Mockito.when(flatFile.getConsumeQueueMaxOffset()).thenReturn(101L);
        Mockito.when(flatFile.getConsumeQueueCommitOffset()).thenReturn(101L);
        dispatcher.doScheduleDispatch(flatFile, true).join();

        Assert.assertTrue(dispatcher.getFailedGroupCommitMap().isEmpty());
        Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50))
            .atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Assert.assertTrue("preflight buffer was not released", preflight.hasReleased());
                Assert.assertTrue("failed context buffer was not released after the retry committed", loopResult.hasReleased());
            });
    }

    @Test
    public void dispatchConsumeQueueAppendFailureDoesNotRescheduleImmediatelyTest() throws Exception {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(101L);

        FlatFileInterface flatFile = mockInitializedFlatFile();
        Mockito.when(flatFile.appendCommitLog(any(SelectMappedBufferResult.class))).thenReturn(AppendResult.SUCCESS);
        Mockito.when(flatFile.appendConsumeQueue(any(DispatchRequest.class))).thenReturn(AppendResult.UNKNOWN_ERROR);
        MessageStoreDispatcherImpl dispatcher =
            Mockito.spy(buildMockedDispatcher(defaultStore, Mockito.mock(IndexService.class)));

        // The mocked message has an old timestamp, so the batch is timed out and the normal
        // success path would reschedule itself. A batch with no consume queue entry must not.
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        SelectMappedBufferResult preflight = newMockedBuffer(buffer);
        SelectMappedBufferResult loopResult = newMockedBuffer(buffer);

        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);
        Mockito.when(cq.get(anyLong())).thenReturn(new CqUnit(100, 1000, buffer.remaining(), 0L));
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt()))
            .thenReturn(preflight, loopResult);

        dispatcher.doScheduleDispatch(flatFile, true).join();

        Mockito.verify(dispatcher, Mockito.after(500).never()).dispatch(any(FlatFileInterface.class));
        Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50))
            .atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Assert.assertTrue("preflight buffer was not released", preflight.hasReleased());
                Assert.assertTrue("buffer retained by the commitlog was not released after commit", loopResult.hasReleased());
            });
    }

    @Test
    public void dispatchServiceTest() {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        messageStore = Mockito.mock(TieredMessageStore.class);
        IndexService indexService =
            new IndexStoreService(new FlatFileFactory(metadataStore, storeConfig, executor), storePath);
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
        Mockito.when(messageStore.getIndexService()).thenReturn(indexService);

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
        }).when(dispatcherSpy).dispatch(any(FlatFileInterface.class));
        dispatcherSpy.start();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(result::get);
        dispatcherSpy.shutdown();
    }

    @Test
    public void isMemoryEnoughTest() {
        MessageStore defaultStore = Mockito.mock(MessageStore.class);
        messageStore = Mockito.mock(TieredMessageStore.class);
        IndexService indexService =
            new IndexStoreService(new FlatFileFactory(metadataStore, storeConfig, executor), storePath);
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultStore);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getStoreExecutor()).thenReturn(executor);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(fileStore);
        Mockito.when(messageStore.getIndexService()).thenReturn(indexService);
        MessageStoreDispatcherImpl dispatcher = new MessageStoreDispatcherImpl(messageStore);

        FlatFileInterface flatFile = Mockito.mock(FlatFileInterface.class);
        Mockito.when(flatFile.getMessageQueue()).thenReturn(mq);

        // (1) memory sufficient: threshold = min(maxMemory * 0, 0) = 0 → always true
        storeConfig.setTieredStoreDispatchMinFreeMemoryRatio(0.0f);
        storeConfig.setTieredStoreDispatchMinFreeMemoryMaxBytes(0L);
        Assert.assertTrue(dispatcher.isMemoryEnough(flatFile));

        // (2) memory below threshold: threshold = min(maxMemory * 1.0, MAX_VALUE) = maxMemory → always false
        storeConfig.setTieredStoreDispatchMinFreeMemoryRatio(1.0f);
        storeConfig.setTieredStoreDispatchMinFreeMemoryMaxBytes(Long.MAX_VALUE);
        Assert.assertFalse(dispatcher.isMemoryEnough(flatFile));

        // (3) maxBytes cap: ratio = 1.0 → ratioThreshold = maxMemory
        // maxBytes = 1 → threshold = min(maxMemory, 1) = 1 → available > 1 → true
        storeConfig.setTieredStoreDispatchMinFreeMemoryMaxBytes(1L);
        Assert.assertTrue(dispatcher.isMemoryEnough(flatFile));
        // maxBytes = MAX_VALUE → threshold = maxMemory → available < maxMemory → false
        storeConfig.setTieredStoreDispatchMinFreeMemoryMaxBytes(Long.MAX_VALUE);
        Assert.assertFalse(dispatcher.isMemoryEnough(flatFile));

        dispatcher.shutdown();
    }
}
