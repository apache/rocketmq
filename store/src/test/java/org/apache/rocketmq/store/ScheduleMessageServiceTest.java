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

package org.apache.rocketmq.store;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScheduleMessageServiceTest {

    private Random random = new Random();

    @Test
    public void testCorrectDelayOffset_whenInit() throws Exception {

        ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = null;

        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(buildMessageStoreConfig(),
            new BrokerStatsManager("simpleTest", true), null, new BrokerConfig());
        ScheduleMessageService scheduleMessageService = new ScheduleMessageService(defaultMessageStore);
        scheduleMessageService.parseDelayLevel();

        ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable1 = new ConcurrentHashMap<>();
        for (int i = 1; i <= 18; i++) {
            offsetTable1.put(i, random.nextLong());
        }

        Field field = scheduleMessageService.getClass().getDeclaredField("offsetTable");
        field.setAccessible(true);
        field.set(scheduleMessageService, offsetTable1);

        String jsonStr = scheduleMessageService.encode();
        scheduleMessageService.decode(jsonStr);

        offsetTable = (ConcurrentMap<Integer, Long>) field.get(scheduleMessageService);

        for (Map.Entry<Integer, Long> entry : offsetTable.entrySet()) {
            assertEquals(entry.getValue(), offsetTable1.get(entry.getKey()));
        }

        scheduleMessageService.correctDelayOffset();

        offsetTable = (ConcurrentMap<Integer, Long>) field.get(scheduleMessageService);

        for (long offset : offsetTable.values()) {
            assertEquals(0, offset);
        }

    }

    private MessageStoreConfig buildMessageStoreConfig() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        return messageStoreConfig;
    }

    @Test
    public void testHandlePutResultTask() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        MessageStoreConfig config = buildMessageStoreConfig();
        config.setEnableScheduleMessageStats(false);
        config.setEnableScheduleAsyncDeliver(true);
        when(messageStore.getMessageStoreConfig()).thenReturn(config);
        ScheduleMessageService scheduleMessageService = new ScheduleMessageService(messageStore);
        scheduleMessageService.parseDelayLevel();

        Field field = scheduleMessageService.getClass().getDeclaredField("deliverPendingTable");
        field.setAccessible(true);
        Map<Integer /* level */, LinkedBlockingQueue<ScheduleMessageService.PutResultProcess>> deliverPendingTable =
            (Map<Integer, LinkedBlockingQueue<ScheduleMessageService.PutResultProcess>>) field.get(scheduleMessageService);

        field = scheduleMessageService.getClass().getDeclaredField("offsetTable");
        field.setAccessible(true);
        ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            (ConcurrentMap<Integer /* level */, Long/* offset */>) field.get(scheduleMessageService);
        for (int i = 1; i <= scheduleMessageService.getMaxDelayLevel(); i++) {
            offsetTable.put(i, 0L);
        }

        int deliverThreadPoolNums = Runtime.getRuntime().availableProcessors();
        ScheduledExecutorService handleExecutorService = new ScheduledThreadPoolExecutor(deliverThreadPoolNums,
            new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
        field = scheduleMessageService.getClass().getDeclaredField("handleExecutorService");
        field.setAccessible(true);
        field.set(scheduleMessageService, handleExecutorService);

        field = scheduleMessageService.getClass().getDeclaredField("started");
        field.setAccessible(true);
        AtomicBoolean started = (AtomicBoolean) field.get(scheduleMessageService);
        started.set(true);

        for (int level = 1; level <= scheduleMessageService.getMaxDelayLevel(); level++) {
            ScheduleMessageService.HandlePutResultTask handlePutResultTask = scheduleMessageService.new HandlePutResultTask(level);
            handleExecutorService.schedule(handlePutResultTask, 10L, TimeUnit.MILLISECONDS);
        }

        MessageExt messageExt = new MessageExt();
        messageExt.putUserProperty("init", "test");
        messageExt.getProperties().put(MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        when(messageStore.lookMessageByOffset(anyLong(), anyInt())).thenReturn(messageExt);
        when(messageStore.putMessage(any())).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, null));

        int msgNum = 100;
        int totalMsgNum = msgNum * scheduleMessageService.getMaxDelayLevel();
        List<CompletableFuture<PutMessageResult>> putMsgFutrueList = new ArrayList<>(totalMsgNum);
        for (int level = 1; level <= scheduleMessageService.getMaxDelayLevel(); level++) {
            for (int num = 0; num < msgNum; num++) {
                CompletableFuture<PutMessageResult> future = new CompletableFuture<>();
                ScheduleMessageService.PutResultProcess putResultProcess = scheduleMessageService.new PutResultProcess();
                putResultProcess = putResultProcess
                    .setOffset(num)
                    .setAutoResend(true)
                    .setFuture(future)
                    .thenProcess();
                deliverPendingTable.get(level).add(putResultProcess);
                putMsgFutrueList.add(future);
            }
        }

        Collections.shuffle(putMsgFutrueList);
        Random random = new Random();
        for (CompletableFuture<PutMessageResult> future : putMsgFutrueList) {
            PutMessageStatus status;
            if (random.nextInt(1000) % 2 == 0) {
                status = PutMessageStatus.PUT_OK;
            } else {
                status = PutMessageStatus.OS_PAGECACHE_BUSY;
            }

            if (random.nextInt(1000) % 2 == 0) {
                PutMessageResult result = new PutMessageResult(status, null);
                future.complete(result);
            } else {
                future.completeExceptionally(new Throwable("complete exceptionally"));
            }
        }

        Thread.sleep(1000);
        for (int level = 1; level <= scheduleMessageService.getMaxDelayLevel(); level++) {
            Assert.assertEquals(0, deliverPendingTable.get(level).size());
            Assert.assertEquals(msgNum, offsetTable.get(level).longValue());
        }

        scheduleMessageService.shutdown();
    }
}
