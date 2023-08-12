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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;

public  class CleanConsumeQueueService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private long lastPhysicalMinOffset = 0;

    private final DefaultMessageStore messageStore;

    public CleanConsumeQueueService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public void run() {
        try {
            this.deleteExpiredFiles();
        } catch (Throwable e) {
            LOGGER.warn(this.getServiceName() + " service has exception. ", e);
        }
    }

    private void deleteExpiredFiles() {
        int deleteLogicsFilesInterval = messageStore.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

        long minOffset = messageStore.getCommitLog().getMinOffset();
        if (minOffset > this.lastPhysicalMinOffset) {
            this.lastPhysicalMinOffset = minOffset;

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = messageStore.getConsumeQueueTable();

            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface logic : maps.values()) {
                    int deleteCount = messageStore.getConsumeQueueStore().deleteExpiredFile(logic, minOffset);
                    if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                        try {
                            Thread.sleep(deleteLogicsFilesInterval);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }

            messageStore.getIndexService().deleteExpiredFile(minOffset);
        }
    }

    public String getServiceName() {
        return messageStore.getBrokerConfig().getIdentifier() + CleanConsumeQueueService.class.getSimpleName();
    }
}

