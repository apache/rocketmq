/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.transaction;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public class TransactionDataManager implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected final AtomicLong maxTransactionDataExpireTime = new AtomicLong(System.currentTimeMillis());
    protected final Map<String /* producerGroup@transactionId */, NavigableSet<TransactionData>> transactionIdDataMap = new ConcurrentHashMap<>();
    protected final TransactionDataCleaner transactionDataCleaner = new TransactionDataCleaner();

    protected String buildKey(String producerGroup, String transactionId) {
        return producerGroup + "@" + transactionId;
    }

    public void addTransactionData(String producerGroup, String transactionId, TransactionData transactionData) {
        this.transactionIdDataMap.compute(buildKey(producerGroup, transactionId), (key, dataSet) -> {
            if (dataSet == null) {
                dataSet = new ConcurrentSkipListSet<>();
            }
            dataSet.add(transactionData);
            if (dataSet.size() > ConfigurationManager.getProxyConfig().getTransactionDataMaxNum()) {
                dataSet.pollFirst();
            }
            return dataSet;
        });
    }

    public TransactionData pollNoExpireTransactionData(String producerGroup, String transactionId) {
        AtomicReference<TransactionData> res = new AtomicReference<>();
        long currTimestamp = System.currentTimeMillis();
        this.transactionIdDataMap.computeIfPresent(buildKey(producerGroup, transactionId), (key, dataSet) -> {
            TransactionData data = dataSet.pollLast();
            while (data != null && data.getExpireTime() < currTimestamp) {
                data = dataSet.pollLast();
            }
            if (data != null) {
                res.set(data);
            }
            if (dataSet.isEmpty()) {
                return null;
            }
            return dataSet;
        });
        return res.get();
    }

    public void removeTransactionData(String producerGroup, String transactionId, TransactionData transactionData) {
        this.transactionIdDataMap.computeIfPresent(buildKey(producerGroup, transactionId), (key, dataSet) -> {
            dataSet.remove(transactionData);
            if (dataSet.isEmpty()) {
                return null;
            }
            return dataSet;
        });
    }

    protected void cleanExpireTransactionData() {
        long currTimestamp = System.currentTimeMillis();
        Set<String> transactionIdSet = this.transactionIdDataMap.keySet();
        for (String transactionId : transactionIdSet) {
            this.transactionIdDataMap.computeIfPresent(transactionId, (transactionIdKey, dataSet) -> {
                Iterator<TransactionData> iterator = dataSet.iterator();
                while (iterator.hasNext()) {
                    try {
                        TransactionData data = iterator.next();
                        if (data.getExpireTime() < currTimestamp) {
                            iterator.remove();
                        } else {
                            break;
                        }
                    } catch (NoSuchElementException ignore) {
                        break;
                    }
                }
                if (dataSet.isEmpty()) {
                    return null;
                }
                try {
                    TransactionData maxData = dataSet.last();
                    maxTransactionDataExpireTime.set(Math.max(maxTransactionDataExpireTime.get(), maxData.getExpireTime()));
                } catch (NoSuchElementException ignore) {
                }
                return dataSet;
            });
        }
    }

    protected class TransactionDataCleaner extends ServiceThread {

        @Override
        public String getServiceName() {
            return "TransactionDataCleaner";
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                this.waitForRunning(ConfigurationManager.getProxyConfig().getTransactionDataExpireScanPeriodMillis());
            }
            log.info(this.getServiceName() + " service stopped");
        }

        @Override
        protected void onWaitEnd() {
            cleanExpireTransactionData();
        }
    }

    protected void waitTransactionDataClear() throws InterruptedException {
        this.cleanExpireTransactionData();
        long waitMs = Math.max(this.maxTransactionDataExpireTime.get() - System.currentTimeMillis(), 0);
        waitMs = Math.min(waitMs, ConfigurationManager.getProxyConfig().getTransactionDataMaxWaitClearMillis());

        if (waitMs > 0) {
            TimeUnit.MILLISECONDS.sleep(waitMs);
        }
    }

    @Override
    public void shutdown() throws Exception {
        this.transactionDataCleaner.shutdown();
        this.waitTransactionDataClear();
    }

    @Override
    public void start() throws Exception {
        this.transactionDataCleaner.start();
    }
}
