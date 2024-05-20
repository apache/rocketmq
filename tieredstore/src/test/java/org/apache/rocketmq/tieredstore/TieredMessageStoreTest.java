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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcher;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class TieredMessageStoreTest {

    private final String brokerName = "brokerName";
    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private final MessageQueue mq = new MessageQueue("MessageStoreTest", brokerName, 0);

    private Configuration configuration;
    private DefaultMessageStore defaultStore;
    private TieredMessageStore currentStore;
    private FlatFileStore flatFileStore;
    private MessageStoreFetcher fetcher;

    @Before
    public void init() throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(brokerName);

        Properties properties = new Properties();
        properties.setProperty("recordGetMessageResult", Boolean.TRUE.toString().toLowerCase(Locale.ROOT));
        properties.setProperty("tieredBackendServiceProvider", PosixFileSegment.class.getName());

        configuration = new Configuration(LoggerFactory.getLogger(
            MessageStoreUtil.TIERED_STORE_LOGGER_NAME), storePath + File.separator + "conf",
            new org.apache.rocketmq.tieredstore.MessageStoreConfig(), brokerConfig);
        configuration.registerConfig(properties);

        MessageStorePluginContext context = new MessageStorePluginContext(
            new MessageStoreConfig(), null, null, brokerConfig, configuration);

        defaultStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(defaultStore.load()).thenReturn(true);

        currentStore = new TieredMessageStore(context, defaultStore);
        Assert.assertNotNull(currentStore.getStoreConfig());
        Assert.assertNotNull(currentStore.getBrokerName());
        Assert.assertEquals(defaultStore, currentStore.getDefaultStore());
        Assert.assertNotNull(currentStore.getMetadataStore());
        Assert.assertNotNull(currentStore.getTopicFilter());
        Assert.assertNotNull(currentStore.getStoreExecutor());
        Assert.assertNotNull(currentStore.getFlatFileStore());
        Assert.assertNotNull(currentStore.getIndexService());

        fetcher = Mockito.spy(currentStore.fetcher);
        try {
            Field field = currentStore.getClass().getDeclaredField("fetcher");
            field.setAccessible(true);
            field.set(currentStore, fetcher);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getClass().getCanonicalName() + ": " + e.getMessage());
        }

        flatFileStore = currentStore.getFlatFileStore();

        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Mockito.when(defaultStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(200L);
        ConsumeQueueInterface cq = Mockito.mock(ConsumeQueueInterface.class);
        Mockito.when(defaultStore.getConsumeQueue(anyString(), anyInt())).thenReturn(cq);

        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        Mockito.when(cq.get(anyLong())).thenReturn(
            new CqUnit(100, 1000, buffer.remaining(), 0L));
        Mockito.when(defaultStore.selectOneMessageByOffset(anyLong(), anyInt())).thenReturn(
            new SelectMappedBufferResult(0L, buffer.asReadOnlyBuffer(), buffer.remaining(), null));
        currentStore.load();

        FlatMessageFile flatFile = currentStore.getFlatFileStore().computeIfAbsent(mq);
        Assert.assertNotNull(flatFile);
        currentStore.dispatcher.doScheduleDispatch(flatFile, true).join();

        for (int i = 100; i < 200; i++) {
            SelectMappedBufferResult bufferResult = new SelectMappedBufferResult(
                0L, buffer, buffer.remaining(), null);
            DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(),
                MessageFormatUtil.getCommitLogOffset(buffer), buffer.remaining(), 0L,
                MessageFormatUtil.getStoreTimeStamp(buffer), 0L,
                "", "", 0, 0L, new HashMap<>());
            flatFile.appendCommitLog(bufferResult);
            flatFile.appendConsumeQueue(request);
        }
        currentStore.dispatcher.doScheduleDispatch(flatFile, true).join();
    }

    @After
    public void shutdown() throws IOException {
        currentStore.shutdown();
        currentStore.destroy();
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void testViaTieredStorage() {
        Properties properties = new Properties();

        // TieredStorageLevel.DISABLE
        properties.setProperty("tieredStorageLevel", "0");
        configuration.update(properties);
        Assert.assertFalse(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        // TieredStorageLevel.NOT_IN_DISK
        properties.setProperty("tieredStorageLevel", "1");
        configuration.update(properties);
        when(defaultStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(false);
        Assert.assertTrue(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        when(defaultStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Assert.assertFalse(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        // TieredStorageLevel.NOT_IN_MEM
        properties.setProperty("tieredStorageLevel", "2");
        configuration.update(properties);
        Mockito.when(defaultStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(false);
        Mockito.when(defaultStore.checkInMemByConsumeOffset(anyString(), anyInt(), anyLong(), anyInt())).thenReturn(true);
        Assert.assertTrue(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        Mockito.when(defaultStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Mockito.when(defaultStore.checkInMemByConsumeOffset(anyString(), anyInt(), anyLong(), anyInt())).thenReturn(false);
        Assert.assertTrue(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        Mockito.when(defaultStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Mockito.when(defaultStore.checkInMemByConsumeOffset(anyString(), anyInt(), anyLong(), anyInt())).thenReturn(true);
        Assert.assertFalse(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        // TieredStorageLevel.FORCE
        properties.setProperty("tieredStorageLevel", "3");
        configuration.update(properties);
        Assert.assertTrue(currentStore.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));
    }

    @Test
    public void testGetMessageAsync() {
        GetMessageResult expect = new GetMessageResult();
        expect.setStatus(GetMessageStatus.FOUND);
        expect.setMinOffset(100L);
        expect.setMaxOffset(200L);

        // topic filter
        Mockito.when(defaultStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(expect));
        String groupName = "groupName";
        GetMessageResult result = currentStore.getMessage(
            groupName, TopicValidator.SYSTEM_TOPIC_PREFIX, mq.getQueueId(), 100, 0, null);
        Assert.assertSame(expect, result);

        // fetch from default
        Mockito.when(fetcher.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(expect));

        result = currentStore.getMessage(
            groupName, mq.getTopic(), mq.getQueueId(), 100, 0, null);
        Assert.assertSame(expect, result);

        expect.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
        Assert.assertSame(expect, currentStore.getMessage(
            groupName, mq.getTopic(), mq.getQueueId(), 0, 0, null));

        expect.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
        Assert.assertSame(expect, currentStore.getMessage(
            groupName, mq.getTopic(), mq.getQueueId(), 0, 0, null));

        expect.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
        Assert.assertSame(expect, currentStore.getMessage(
            groupName, mq.getTopic(), mq.getQueueId(), 0, 0, null));

        expect.setStatus(GetMessageStatus.OFFSET_RESET);
        Assert.assertSame(expect, currentStore.getMessage(
            groupName, mq.getTopic(), mq.getQueueId(), 0, 0, null));
    }

    @Test
    public void testGetMinOffsetInQueue() {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(mq);
        Mockito.when(defaultStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        Assert.assertEquals(100L, currentStore.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId()));

        Mockito.when(flatFile.getConsumeQueueMinOffset()).thenReturn(10L);
        Assert.assertEquals(10L, currentStore.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId()));
    }

    @Test
    public void testGetEarliestMessageTimeAsync() {
        when(fetcher.getEarliestMessageTimeAsync(anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(1L));
        Assert.assertEquals(1, (long) currentStore.getEarliestMessageTimeAsync(mq.getTopic(), mq.getQueueId()).join());

        when(fetcher.getEarliestMessageTimeAsync(anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(-1L));
        when(defaultStore.getEarliestMessageTime(anyString(), anyInt())).thenReturn(2L);
        Assert.assertEquals(2, (long) currentStore.getEarliestMessageTimeAsync(mq.getTopic(), mq.getQueueId()).join());
    }

    @Test
    public void testGetMessageStoreTimeStampAsync() {
        // TieredStorageLevel.DISABLE
        Properties properties = new Properties();
        properties.setProperty("tieredStorageLevel", "DISABLE");
        configuration.update(properties);
        when(fetcher.getMessageStoreTimeStampAsync(anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(1L));
        when(defaultStore.getMessageStoreTimeStampAsync(anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(2L));
        when(defaultStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(3L);
        Assert.assertEquals(2, (long) currentStore.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join());

        // TieredStorageLevel.FORCE
        properties.setProperty("tieredStorageLevel", "FORCE");
        configuration.update(properties);
        Assert.assertEquals(1, (long) currentStore.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join());

        Mockito.when(fetcher.getMessageStoreTimeStampAsync(anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(-1L));
        Assert.assertEquals(3, (long) currentStore.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join());
    }

    @Test
    public void testGetOffsetInQueueByTime() {
        Properties properties = new Properties();
        properties.setProperty("tieredStorageLevel", "FORCE");
        configuration.update(properties);

        Mockito.when(fetcher.getOffsetInQueueByTime(anyString(), anyInt(), anyLong(), eq(BoundaryType.LOWER))).thenReturn(1L);
        Mockito.when(defaultStore.getOffsetInQueueByTime(anyString(), anyInt(), anyLong())).thenReturn(2L);
        Mockito.when(defaultStore.getEarliestMessageTime()).thenReturn(100L);
        Assert.assertEquals(1L, currentStore.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 1000, BoundaryType.LOWER));
        Assert.assertEquals(1L, currentStore.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));

        Mockito.when(fetcher.getOffsetInQueueByTime(anyString(), anyInt(), anyLong(), eq(BoundaryType.LOWER))).thenReturn(-1L);
        Assert.assertEquals(-1L, currentStore.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0));
        Assert.assertEquals(-1L, currentStore.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));
    }

    @Test
    public void testQueryMessage() {
        QueryMessageResult result1 = new QueryMessageResult();
        result1.addMessage(new SelectMappedBufferResult(0, null, 0, null));
        result1.addMessage(new SelectMappedBufferResult(0, null, 0, null));
        when(fetcher.queryMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(result1));
        QueryMessageResult result2 = new QueryMessageResult();
        result2.addMessage(new SelectMappedBufferResult(0, null, 0, null));
        when(defaultStore.queryMessage(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(result2);
        when(defaultStore.getEarliestMessageTime()).thenReturn(100L);
        Assert.assertEquals(2, currentStore.queryMessage(mq.getTopic(), "key", 32, 0, 99).getMessageMapedList().size());
        Assert.assertEquals(1, currentStore.queryMessage(mq.getTopic(), "key", 32, 100, 200).getMessageMapedList().size());
        Assert.assertEquals(3, currentStore.queryMessage(mq.getTopic(), "key", 32, 0, 200).getMessageMapedList().size());
    }

    @Test
    public void testCleanUnusedTopics() {
        Set<String> topicSet = new HashSet<>();
        currentStore.cleanUnusedTopic(topicSet);
        Assert.assertNull(flatFileStore.getFlatFile(mq));
        Assert.assertNull(flatFileStore.getMetadataStore().getTopic(mq.getTopic()));
        Assert.assertNull(flatFileStore.getMetadataStore().getQueue(mq));
    }

    @Test
    public void testDeleteTopics() {
        Set<String> topicSet = new HashSet<>();
        topicSet.add(mq.getTopic());
        currentStore.deleteTopics(topicSet);
        Assert.assertNull(flatFileStore.getFlatFile(mq));
        Assert.assertNull(flatFileStore.getMetadataStore().getTopic(mq.getTopic()));
        Assert.assertNull(flatFileStore.getMetadataStore().getQueue(mq));
    }

    @Test
    public void testMetrics() {
        currentStore.getMetricsView();
        currentStore.initMetrics(
            OpenTelemetrySdk.builder().build().getMeter(""), Attributes::builder);
    }
}
