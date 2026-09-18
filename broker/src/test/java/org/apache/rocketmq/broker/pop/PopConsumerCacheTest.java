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

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

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

        // timeout cleanup is skipped when tryLock fails: concurrent pop holds the lock
        record = new PopConsumerRecord(System.currentTimeMillis(),
            groupId, topicId, queueId, 0, 20000, 105, attemptId);
        consumerCache.writeRecords(Collections.singletonList(record));
        Mockito.when(consumerLockService.tryLock(eq(groupId), eq(topicId))).thenReturn(false);
        int remain = consumerCache.cleanupRecords(consumerRecordList::add);
        Assert.assertEquals(1, remain);
        Assert.assertEquals(1, consumerCache.getCacheKeySize());
        Assert.assertEquals(1, consumerCache.getPopInFlightMessageCount(groupId, topicId, queueId));

        // timeout cleanup proceeds and persists records to KV store when the lock is acquired
        Mockito.when(consumerLockService.tryLock(eq(groupId), eq(topicId))).thenReturn(true);
        consumerCache.cleanupRecords(consumerRecordList::add);
        Assert.assertEquals(0, consumerCache.getCacheKeySize());
        Mockito.verify(consumerKVStore).writeRecords(Collections.singletonList(record));
    }

    @Test
    public void commitPendingOffsetTest() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        PopConsumerKVStore consumerKVStore = Mockito.mock(PopConsumerRocksdbStore.class);
        PopConsumerLockService consumerLockService = Mockito.mock(PopConsumerLockService.class);
        ConsumerOffsetManager consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        Mockito.when(consumerLockService.tryLock(groupId, topicId)).thenReturn(true);

        PopConsumerCache consumerCache =
            new PopConsumerCache(brokerController, consumerKVStore, consumerLockService, null);

        // the record is still in cache, so it bounds the commit
        PopConsumerRecord record = new PopConsumerRecord(System.currentTimeMillis(),
            groupId, topicId, queueId, 0, 20000, 100, attemptId);
        consumerCache.writeRecords(Collections.singletonList(record));
        consumerCache.setPendingCommitOffset(groupId, topicId, queueId, 110L);
        consumerCache.cleanupRecords(consumerRecord -> {
        });
        Mockito.verify(consumerOffsetManager).commitOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(100L));

        // nothing remains in cache after the acknowledgement, so the pending offset is committed
        Assert.assertTrue(consumerCache.deleteRecords(Collections.singletonList(record)).isEmpty());
        consumerCache.cleanupRecords(consumerRecord -> {
        });
        Mockito.verify(consumerOffsetManager).commitOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(110L));

        // the offset store has caught up, so the pending offset is not committed again
        Mockito.when(consumerOffsetManager.queryOffset(groupId, topicId, queueId)).thenReturn(110L);
        consumerCache.cleanupRecords(consumerRecord -> {
        });
        Mockito.verify(consumerOffsetManager, Mockito.times(1)).commitOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(110L));

        // reset offset wins over the pending offset
        Mockito.when(consumerOffsetManager.hasOffsetReset(topicId, groupId, queueId)).thenReturn(true);
        consumerCache.setPendingCommitOffset(groupId, topicId, queueId, 120L);
        consumerCache.cleanupRecords(consumerRecord -> {
        });
        Mockito.verify(consumerOffsetManager, Mockito.never()).commitOffset(
            anyString(), anyString(), anyString(), anyInt(), eq(120L));

        // records of an offline consumer are persisted before the entry is dropped
        Mockito.when(consumerOffsetManager.hasOffsetReset(topicId, groupId, queueId)).thenReturn(false);
        Mockito.when(consumerLockService.isLockTimeout(any(), any())).thenReturn(true);
        consumerCache.writeRecords(Collections.singletonList(new PopConsumerRecord(
            System.currentTimeMillis(), groupId, topicId, queueId, 0, 20000, 200, attemptId)));
        consumerCache.setPendingCommitOffset(groupId, topicId, queueId, 210L);
        consumerCache.cleanupRecords(consumerRecord -> {
        });
        Mockito.verify(consumerKVStore).writeRecords(Mockito.argThat(records ->
            records.size() == 1 && records.get(0).getOffset() == 200L));
        Mockito.verify(consumerOffsetManager).commitOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(210L));
        Assert.assertEquals(0, consumerCache.getCacheKeySize());
    }
}