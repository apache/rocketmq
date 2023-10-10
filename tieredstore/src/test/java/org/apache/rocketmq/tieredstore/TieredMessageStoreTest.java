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
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.file.CompositeQueueFlatFile;
import org.apache.rocketmq.tieredstore.file.TieredFlatFileManager;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.apache.rocketmq.common.BoundaryType;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TieredMessageStoreTest {

    private final String storePath = TieredStoreTestUtil.getRandomStorePath();

    private MessageStoreConfig storeConfig;
    private MessageQueue mq;
    private MessageStore nextStore;
    private TieredMessageStore store;
    private TieredMessageFetcher fetcher;
    private Configuration configuration;
    private TieredFlatFileManager flatFileManager;

    @Before
    public void setUp() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        mq = new MessageQueue("TieredMessageStoreTest", "broker", 0);

        nextStore = Mockito.mock(DefaultMessageStore.class);
        CommitLog commitLog = mock(CommitLog.class);
        when(commitLog.getMinOffset()).thenReturn(100L);
        when(nextStore.getCommitLog()).thenReturn(commitLog);

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("broker");
        configuration = new Configuration(LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME), "/tmp/rmqut/config", storeConfig, brokerConfig);
        Properties properties = new Properties();
        properties.setProperty("tieredBackendServiceProvider", "org.apache.rocketmq.tieredstore.provider.memory.MemoryFileSegment");
        configuration.registerConfig(properties);
        MessageStorePluginContext context = new MessageStorePluginContext(new MessageStoreConfig(), null, null, brokerConfig, configuration);

        store = new TieredMessageStore(context, nextStore);

        fetcher = Mockito.mock(TieredMessageFetcher.class);
        try {
            Field field = store.getClass().getDeclaredField("fetcher");
            field.setAccessible(true);
            field.set(store, fetcher);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getClass().getCanonicalName() + ": " + e.getMessage());
        }

        TieredFlatFileManager.getInstance(store.getStoreConfig()).getOrCreateFlatFileIfAbsent(mq);
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreExecutor.shutdown();
        TieredStoreTestUtil.destroyCompositeFlatFileManager();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
    }

    private void mockCompositeFlatFile() {
        flatFileManager = Mockito.mock(TieredFlatFileManager.class);
        CompositeQueueFlatFile flatFile = Mockito.mock(CompositeQueueFlatFile.class);
        when(flatFile.getConsumeQueueCommitOffset()).thenReturn(Long.MAX_VALUE);
        when(flatFileManager.getFlatFile(mq)).thenReturn(flatFile);
        try {
            Field field = store.getClass().getDeclaredField("flatFileManager");
            field.setAccessible(true);
            field.set(store, flatFileManager);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getClass().getCanonicalName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testViaTieredStorage() {
        mockCompositeFlatFile();
        Properties properties = new Properties();
        // TieredStorageLevel.DISABLE
        properties.setProperty("tieredStorageLevel", "0");
        configuration.update(properties);
        Assert.assertFalse(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        // TieredStorageLevel.NOT_IN_DISK
        properties.setProperty("tieredStorageLevel", "1");
        configuration.update(properties);
        when(nextStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(false);
        Assert.assertTrue(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        when(nextStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Assert.assertFalse(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        // TieredStorageLevel.NOT_IN_MEM
        properties.setProperty("tieredStorageLevel", "2");
        configuration.update(properties);
        Mockito.when(nextStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(false);
        Mockito.when(nextStore.checkInMemByConsumeOffset(anyString(), anyInt(), anyLong(), anyInt())).thenReturn(true);
        Assert.assertTrue(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        Mockito.when(nextStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Mockito.when(nextStore.checkInMemByConsumeOffset(anyString(), anyInt(), anyLong(), anyInt())).thenReturn(false);
        Assert.assertTrue(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        Mockito.when(nextStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Mockito.when(nextStore.checkInMemByConsumeOffset(anyString(), anyInt(), anyLong(), anyInt())).thenReturn(true);
        Assert.assertFalse(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));

        // TieredStorageLevel.FORCE
        properties.setProperty("tieredStorageLevel", "3");
        configuration.update(properties);
        Assert.assertTrue(store.fetchFromCurrentStore(mq.getTopic(), mq.getQueueId(), 0));
    }

    @Test
    public void testGetMessageAsync() {
        mockCompositeFlatFile();
        GetMessageResult result1 = new GetMessageResult();
        result1.setStatus(GetMessageStatus.FOUND);
        GetMessageResult result2 = new GetMessageResult();
        result2.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);

        when(fetcher.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(result1));
        when(nextStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(result2);
        Assert.assertSame(result1, store.getMessage("group", mq.getTopic(), mq.getQueueId(), 0, 0, null));

        result1.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
        Assert.assertSame(result1, store.getMessage("group", mq.getTopic(), mq.getQueueId(), 0, 0, null));

        result1.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
        Assert.assertSame(result1, store.getMessage("group", mq.getTopic(), mq.getQueueId(), 0, 0, null));

        result1.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
        Assert.assertSame(result1, store.getMessage("group", mq.getTopic(), mq.getQueueId(), 0, 0, null));

        // TieredStorageLevel.FORCE
        Properties properties = new Properties();
        properties.setProperty("tieredStorageLevel", "3");
        configuration.update(properties);
        when(nextStore.checkInStoreByConsumeOffset(anyString(), anyInt(), anyLong())).thenReturn(true);
        Assert.assertEquals(result2.getStatus(),
            store.getMessage("group", mq.getTopic(), mq.getQueueId(), 0, 0, null).getStatus());
    }

    @Test
    public void testGetEarliestMessageTimeAsync() {
        when(fetcher.getEarliestMessageTimeAsync(anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(1L));
        Assert.assertEquals(1, (long) store.getEarliestMessageTimeAsync(mq.getTopic(), mq.getQueueId()).join());

        when(fetcher.getEarliestMessageTimeAsync(anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(-1L));
        when(nextStore.getEarliestMessageTime(anyString(), anyInt())).thenReturn(2L);
        Assert.assertEquals(2, (long) store.getEarliestMessageTimeAsync(mq.getTopic(), mq.getQueueId()).join());
    }

    @Test
    public void testGetMessageStoreTimeStampAsync() {
        mockCompositeFlatFile();
        // TieredStorageLevel.DISABLE
        Properties properties = new Properties();
        properties.setProperty("tieredStorageLevel", "DISABLE");
        configuration.update(properties);
        when(fetcher.getMessageStoreTimeStampAsync(anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(1L));
        when(nextStore.getMessageStoreTimeStampAsync(anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(2L));
        when(nextStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(3L);
        Assert.assertEquals(2, (long) store.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join());

        // TieredStorageLevel.FORCE
        properties.setProperty("tieredStorageLevel", "FORCE");
        configuration.update(properties);
        Assert.assertEquals(1, (long) store.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join());

        Mockito.when(fetcher.getMessageStoreTimeStampAsync(anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(-1L));
        Assert.assertEquals(3, (long) store.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 0).join());
    }

    @Test
    public void testGetOffsetInQueueByTime() {
        Mockito.when(fetcher.getOffsetInQueueByTime(anyString(), anyInt(), anyLong(), eq(BoundaryType.LOWER))).thenReturn(1L);
        Mockito.when(nextStore.getOffsetInQueueByTime(anyString(), anyInt(), anyLong())).thenReturn(2L);
        Mockito.when(nextStore.getEarliestMessageTime()).thenReturn(100L);
        Assert.assertEquals(1, store.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));
        Assert.assertEquals(2, store.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 1000, BoundaryType.LOWER));

        Mockito.when(fetcher.getOffsetInQueueByTime(anyString(), anyInt(), anyLong(), eq(BoundaryType.LOWER))).thenReturn(-1L);
        Assert.assertEquals(2, store.getOffsetInQueueByTime(mq.getTopic(), mq.getQueueId(), 0, BoundaryType.LOWER));
    }

    @Test
    public void testQueryMessage() {
        QueryMessageResult result1 = new QueryMessageResult();
        result1.addMessage(new SelectMappedBufferResult(0, null, 0, null));
        result1.addMessage(new SelectMappedBufferResult(0, null, 0, null));
        when(fetcher.queryMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(result1));
        QueryMessageResult result2 = new QueryMessageResult();
        result2.addMessage(new SelectMappedBufferResult(0, null, 0, null));
        when(nextStore.queryMessage(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(result2);
        when(nextStore.getEarliestMessageTime()).thenReturn(100L);
        Assert.assertEquals(2, store.queryMessage(mq.getTopic(), "key", 32, 0, 99).getMessageMapedList().size());
        Assert.assertEquals(1, store.queryMessage(mq.getTopic(), "key", 32, 100, 200).getMessageMapedList().size());
        Assert.assertEquals(3, store.queryMessage(mq.getTopic(), "key", 32, 0, 200).getMessageMapedList().size());
    }

    @Test
    public void testGetMinOffsetInQueue() {
        mockCompositeFlatFile();
        CompositeQueueFlatFile flatFile = flatFileManager.getFlatFile(mq);
        when(nextStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        when(flatFileManager.getFlatFile(mq)).thenReturn(null);
        Assert.assertEquals(100L, store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId()));

        when(flatFileManager.getFlatFile(mq)).thenReturn(flatFile);
        when(flatFile.getConsumeQueueMinOffset()).thenReturn(10L);
        Assert.assertEquals(10L, store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId()));
    }

    @Test
    public void testCleanUnusedTopics() {
        Set<String> topicSet = new HashSet<>();
        try {
            store.cleanUnusedTopic(topicSet);
        } catch (Exception e) {
        }
        Assert.assertNull(TieredFlatFileManager.getInstance(store.getStoreConfig()).getFlatFile(mq));
        Assert.assertNull(TieredStoreUtil.getMetadataStore(store.getStoreConfig()).getTopic(mq.getTopic()));
        Assert.assertNull(TieredStoreUtil.getMetadataStore(store.getStoreConfig()).getQueue(mq));
    }

    @Test
    public void testDeleteTopics() {
        Set<String> topicSet = new HashSet<>();
        topicSet.add(mq.getTopic());
        try {
            store.deleteTopics(topicSet);
        } catch (Exception e) {
        }
        Assert.assertNull(TieredFlatFileManager.getInstance(store.getStoreConfig()).getFlatFile(mq));
        Assert.assertNull(TieredStoreUtil.getMetadataStore(store.getStoreConfig()).getTopic(mq.getTopic()));
        Assert.assertNull(TieredStoreUtil.getMetadataStore(store.getStoreConfig()).getQueue(mq));
    }

    @Test
    public void testMetrics() {
        store.getMetricsView();
        store.initMetrics(OpenTelemetrySdk.builder().build().getMeter(""),
            Attributes::builder);
    }

    @Test
    public void testShutdownAndDestroy() {
        store.shutdown();
        store.destroy();
    }
}
