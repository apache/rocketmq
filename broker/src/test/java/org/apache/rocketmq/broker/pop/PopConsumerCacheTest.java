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
package org.apache.rocketmq.broker.pop;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class PopConsumerCacheTest {

    private final String attemptId = "attemptId";
    private final String topicId = "TopicTest";
    private final String groupId = "GroupTest";
    private final int queueId = 2;

    @Test
    public void consumerRecordsTest() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setPopConsumerKVServiceLog(true);
        PopConsumerCache.ConsumerRecords consumerRecords =
            new PopConsumerCache.ConsumerRecords(brokerConfig, groupId, topicId, queueId);
        Assert.assertNotNull(consumerRecords.toString());

        for (int i = 0; i < 5; i++) {
            consumerRecords.write(new PopConsumerRecord(i, groupId, topicId, queueId, 0,
                20000, 100 + i, attemptId));
        }
        Assert.assertEquals(100, consumerRecords.getMinOffsetInBuffer());
        Assert.assertEquals(5, consumerRecords.getInFlightRecordCount());

        for (int i = 0; i < 2; i++) {
            consumerRecords.delete(new PopConsumerRecord(i, groupId, topicId, queueId, 0,
                20000, 100 + i, attemptId));
        }
        Assert.assertEquals(102, consumerRecords.getMinOffsetInBuffer());
        Assert.assertEquals(3, consumerRecords.getInFlightRecordCount());

        long bufferTimeout = brokerConfig.getPopCkStayBufferTime();
        consumerRecords.stageExpiredRecords(bufferTimeout + 2);
        Assert.assertEquals(1, consumerRecords.getRemoveTreeMap().size());
        consumerRecords.clearStagedRecords();
        consumerRecords.stageExpiredRecords(bufferTimeout + 4);
        Assert.assertEquals(2, consumerRecords.getRemoveTreeMap().size());
        consumerRecords.clearStagedRecords();
    }

    @Test
    public void consumerOffsetTest() throws IllegalAccessException {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        ConsumerOffsetManager consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        Mockito.when(consumerLockService.tryLock(groupId, topicId)).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        consumerCache.commitOffset("CommitOffsetTest", groupId, topicId, queueId, 100L);
        consumerCache.removeRecords(groupId, topicId, queueId);

        AtomicInteger estimateCacheSize = (AtomicInteger) FieldUtils.readField(
            consumerCache, "estimateCacheSize", true);
        estimateCacheSize.set(2);
        consumerCache.start();
        Awaitility.await().until(() -> estimateCacheSize.get() == 0);
        consumerCache.shutdown();
    }

    @Test
    public void consumerCacheTest() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        Assert.assertEquals(-1L, consumerCache.getMinOffsetInCache(groupId, topicId, queueId));
        Assert.assertEquals(0, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
        Assert.assertEquals(0, consumerCache.getCacheKeySize());

        // write
        for (int i = 0; i < 3; i++) {
            PopConsumerRecord record = new PopConsumerRecord(2L, groupId, topicId, queueId,
                0, 20000, 100 + i, attemptId);
            Assert.assertEquals(consumerCache.getKey(record), consumerCache.getKey(groupId, topicId, queueId));
            consumerCache.writeRecords(Collections.singletonList(record));
        }
        Assert.assertEquals(100, consumerCache.getMinOffsetInCache(groupId, topicId, queueId));
        Assert.assertEquals(3, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
        Assert.assertEquals(1, consumerCache.getCacheKeySize());
        Assert.assertEquals(3, consumerCache.getCacheSize());
        Assert.assertFalse(consumerCache.isCacheFull());

        // delete
        PopConsumerRecord record = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        Assert.assertEquals(0, consumerCache.deleteRecords(Collections.singletonList(record)).size());
        Assert.assertEquals(101, consumerCache.getMinOffsetInCache(groupId, topicId, queueId));
        Assert.assertEquals(2, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
        Assert.assertEquals(2, consumerCache.getCacheSize());

        record = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 104, attemptId);
        Assert.assertEquals(1, consumerCache.deleteRecords(Collections.singletonList(record)).size());
        Assert.assertEquals(101, consumerCache.getMinOffsetInCache(groupId, topicId, queueId));
        Assert.assertEquals(2, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));

        // clean expired records
        Queue<PopConsumerRecord> consumerRecordList = new LinkedBlockingQueue<>();
        consumerCache.cleanupRecords(consumerRecordList::add);
        Assert.assertEquals(2, consumerRecordList.size());

        // clean all
        Mockito.when(consumerLockService.isLockTimeout(any(), any())).thenReturn(true);
        consumerRecordList.clear();
        consumerCache.cleanupRecords(consumerRecordList::add);
        Assert.assertEquals(0, consumerRecordList.size());
    }

    @Test
    public void cleanupRecordsShouldCommitOffsetWhileHoldingConsumerLock() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setPopCkStayBufferTime(60000);
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        ConsumerOffsetManager consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord record = new PopConsumerRecord(System.currentTimeMillis(), groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        consumerCache.writeRecords(Collections.singletonList(record));

        int remain = consumerCache.cleanupRecords(ignored -> Assert.fail("Record should remain in cache"));

        Assert.assertEquals(1, remain);
        Mockito.verify(consumerOffsetManager).commitOffset("PopConsumerCache", groupId, topicId, queueId, 100L);
    }

    @Test
    public void writeAndDeleteRecordsShouldSkipStoreDeleteForBufferedRecords() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord bufferedOldRecord = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        PopConsumerRecord storeOldRecord = new PopConsumerRecord(3L, groupId, topicId, queueId,
            0, 20000, 101, attemptId);
        PopConsumerRecord newRecord = new PopConsumerRecord(4L, groupId, topicId, queueId,
            0, 30000, 100, attemptId);
        consumerCache.writeRecords(Collections.singletonList(bufferedOldRecord));

        consumerCache.writeAndDeleteRecords(Collections.singletonList(newRecord),
            Arrays.asList(bufferedOldRecord, storeOldRecord));

        ArgumentCaptor<List<PopConsumerRecord>> writeCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<PopConsumerRecord>> deleteCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(consumerKVStore).writeAndDeleteRecords(writeCaptor.capture(), deleteCaptor.capture());
        Assert.assertEquals(Collections.singletonList(newRecord), writeCaptor.getValue());
        Assert.assertEquals(Collections.singletonList(storeOldRecord), deleteCaptor.getValue());
        Assert.assertEquals(0, consumerCache.getCacheSize());
        Assert.assertEquals(0, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
    }

    @Test
    public void writeAndDeleteRecordsShouldUseSingleConsumerLock() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord oldRecord1 = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        PopConsumerRecord oldRecord2 = new PopConsumerRecord(3L, groupId, topicId, queueId + 1,
            0, 20000, 101, attemptId);
        PopConsumerRecord newRecord1 = new PopConsumerRecord(4L, groupId, topicId, queueId,
            0, 30000, 100, attemptId);
        PopConsumerRecord newRecord2 = new PopConsumerRecord(5L, groupId, topicId, queueId + 1,
            0, 30000, 101, attemptId);

        consumerCache.writeAndDeleteRecords(Arrays.asList(newRecord1, newRecord2),
            Arrays.asList(oldRecord1, oldRecord2));

        Mockito.verify(consumerLockService, Mockito.times(1)).tryLock(groupId, topicId);
        Mockito.verify(consumerLockService, Mockito.times(1)).unlock(groupId, topicId);
    }

    @Test
    public void writeAndDeleteRecordsShouldLockNormalTopicForRetryTopic() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        String retryTopic = KeyBuilder.buildPopRetryTopicV2(topicId, groupId);
        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord oldRecord = new PopConsumerRecord(2L, groupId, retryTopic, queueId,
            0, 20000, 100, attemptId);
        PopConsumerRecord newRecord = new PopConsumerRecord(4L, groupId, retryTopic, queueId,
            0, 30000, 100, attemptId);

        consumerCache.writeAndDeleteRecords(Collections.singletonList(newRecord),
            Collections.singletonList(oldRecord));

        Mockito.verify(consumerLockService, Mockito.times(1)).tryLock(groupId, topicId);
        Mockito.verify(consumerLockService, Mockito.times(1)).unlock(groupId, topicId);
    }

    @Test
    public void writeAndDeleteRecordsShouldDeleteBufferOnlyWhenStoreKeyMatchesNewRecord() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord bufferedOldRecord = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        PopConsumerRecord newRecord = new PopConsumerRecord(4L, groupId, topicId, queueId,
            0, 19998, 100, attemptId);
        consumerCache.writeRecords(Collections.singletonList(bufferedOldRecord));

        consumerCache.writeAndDeleteRecords(Collections.singletonList(newRecord),
            Collections.singletonList(bufferedOldRecord));

        ArgumentCaptor<List<PopConsumerRecord>> deleteCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(consumerKVStore).writeAndDeleteRecords(any(), deleteCaptor.capture());
        Assert.assertEquals(Collections.emptyList(), deleteCaptor.getValue());
        Assert.assertEquals(0, consumerCache.getCacheSize());
        Assert.assertEquals(0, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
    }

    @Test
    public void writeAndDeleteRecordsShouldKeepBufferWhenStoreFails() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);
        Mockito.doThrow(new RuntimeException("store failure"))
            .when(consumerKVStore).writeAndDeleteRecords(any(), any());

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord bufferedOldRecord = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        PopConsumerRecord newRecord = new PopConsumerRecord(4L, groupId, topicId, queueId,
            0, 30000, 100, attemptId);
        consumerCache.writeRecords(Collections.singletonList(bufferedOldRecord));

        try {
            consumerCache.writeAndDeleteRecords(Collections.singletonList(newRecord),
                Collections.singletonList(bufferedOldRecord));
            Assert.fail("Should throw store failure");
        } catch (RuntimeException e) {
            Assert.assertEquals("store failure", e.getMessage());
        }

        Assert.assertEquals(1, consumerCache.getCacheSize());
        Assert.assertEquals(1, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
    }

    @Test
    public void writeAndDeleteRecordsShouldSendSameStoreKeyDeleteToStore() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(consumerLockService.tryLock(anyString(), anyString())).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);
        PopConsumerRecord oldRecord = new PopConsumerRecord(2L, groupId, topicId, queueId,
            0, 20000, 100, attemptId);
        PopConsumerRecord newRecord = new PopConsumerRecord(4L, groupId, topicId, queueId,
            0, 19998, 100, attemptId);

        consumerCache.writeAndDeleteRecords(Collections.singletonList(newRecord),
            Collections.singletonList(oldRecord));

        ArgumentCaptor<List<PopConsumerRecord>> deleteCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(consumerKVStore).writeAndDeleteRecords(any(), deleteCaptor.capture());
        Assert.assertEquals(Collections.singletonList(oldRecord), deleteCaptor.getValue());
        Assert.assertEquals(0, consumerCache.getCacheSize());
        Assert.assertEquals(0, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));
    }
}
