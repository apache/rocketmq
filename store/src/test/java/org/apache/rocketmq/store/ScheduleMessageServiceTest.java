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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScheduleMessageServiceTest {

    private Random random = new Random();

    @Test
    public void testCorrectDelayOffset_whenInit() throws Exception {

        ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = null;

        ScheduleMessageService scheduleMessageService = new ScheduleMessageService((DefaultMessageStore) buildMessageStore());
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
            assertEquals(offset, 0);
        }

    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), null, new BrokerConfig());
    }
}
