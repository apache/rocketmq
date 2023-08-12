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
package org.apache.rocketmq.store.service;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;

public class DelayLevelService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore messageStore;
    private int maxDelayLevel;

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<>(32);

    public DelayLevelService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        initDelayLevel();
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    private void initDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = messageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                initDelayLevelItem(levelArray, timeUnitTable, i);
            }
        } catch (Exception e) {
            LOGGER.error("parse message delay level failed. messageDelayLevel = {}", levelString, e);
        }
    }

    private void initDelayLevelItem(String[] levelArray, HashMap<String, Long> timeUnitTable, int i) {
        String value = levelArray[i];
        String ch = value.substring(value.length() - 1);
        Long tu = timeUnitTable.get(ch);

        int level = i + 1;
        if (level > this.maxDelayLevel) {
            this.maxDelayLevel = level;
        }
        long num = Long.parseLong(value.substring(0, value.length() - 1));
        long delayTimeMillis = tu * num;
        this.delayLevelTable.put(level, delayTimeMillis);
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

}
