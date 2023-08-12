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

import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;

public class FlushConsumeQueueService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int RETRY_TIMES_OVER = 3;
    private long lastFlushTimestamp = 0;


    private final DefaultMessageStore messageStore;

    public FlushConsumeQueueService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }


    private void doFlush(int retryTimes) {
        int flushConsumeQueueLeastPages = messageStore.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

        if (retryTimes == RETRY_TIMES_OVER) {
            flushConsumeQueueLeastPages = 0;
        }

        long logicsMsgTimestamp = 0;

        int flushConsumeQueueThoroughInterval = messageStore.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
            this.lastFlushTimestamp = currentTimeMillis;
            flushConsumeQueueLeastPages = 0;
            logicsMsgTimestamp = messageStore.getStoreCheckpoint().getLogicsMsgTimestamp();
        }

        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = messageStore.getConsumeQueueTable();

        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
            for (ConsumeQueueInterface cq : maps.values()) {
                boolean result = false;
                for (int i = 0; i < retryTimes && !result; i++) {
                    result = messageStore.getConsumeQueueStore().flush(cq, flushConsumeQueueLeastPages);
                }
            }
        }

        if (messageStore.getMessageStoreConfig().isEnableCompaction()) {
            messageStore.getCompactionStore().flush(flushConsumeQueueLeastPages);
        }

        if (0 == flushConsumeQueueLeastPages) {
            if (logicsMsgTimestamp > 0) {
                messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
            }
            messageStore.getStoreCheckpoint().flush();
        }
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                int interval = messageStore.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                this.waitForRunning(interval);
                this.doFlush(1);
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        this.doFlush(RETRY_TIMES_OVER);

        LOGGER.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + FlushConsumeQueueService.class.getSimpleName();
        }
        return FlushConsumeQueueService.class.getSimpleName();
    }

    @Override
    public long getJoinTime() {
        return 1000 * 60;
    }
}

