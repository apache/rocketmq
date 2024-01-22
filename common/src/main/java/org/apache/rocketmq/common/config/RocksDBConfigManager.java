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
package org.apache.rocketmq.common.config;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;

import java.util.function.BiConsumer;

public class RocksDBConfigManager {
    protected static final Logger BROKER_LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected volatile boolean isStop = false;
    protected ConfigRocksDBStorage configRocksDBStorage = null;
    private FlushOptions flushOptions = null;
    private volatile long lastFlushMemTableMicroSecond = 0;
    private final long memTableFlushInterval;

    public RocksDBConfigManager(long memTableFlushInterval) {
        this.memTableFlushInterval = memTableFlushInterval;
    }

    public boolean load(String configFilePath, BiConsumer<byte[], byte[]> biConsumer) {
        this.isStop = false;
        this.configRocksDBStorage = new ConfigRocksDBStorage(configFilePath);
        if (!this.configRocksDBStorage.start()) {
            return false;
        }
        RocksIterator iterator = this.configRocksDBStorage.iterator();
        try {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                biConsumer.accept(iterator.key(), iterator.value());
                iterator.next();
            }
        } finally {
            iterator.close();
        }

        this.flushOptions = new FlushOptions();
        this.flushOptions.setWaitForFlush(false);
        this.flushOptions.setAllowWriteStall(false);
        return true;
    }

    public void start() {
    }

    public boolean stop() {
        this.isStop = true;
        if (this.configRocksDBStorage != null) {
            return this.configRocksDBStorage.shutdown();
        }
        if (this.flushOptions != null) {
            this.flushOptions.close();
        }
        return true;
    }

    public void flushWAL() {
        try {
            if (this.isStop) {
                return;
            }
            if (this.configRocksDBStorage != null) {
                this.configRocksDBStorage.flushWAL();

                long now = System.currentTimeMillis();
                if (now > this.lastFlushMemTableMicroSecond + this.memTableFlushInterval) {
                    this.configRocksDBStorage.flush(this.flushOptions);
                    this.lastFlushMemTableMicroSecond = now;
                }
            }
        } catch (Exception e) {
            BROKER_LOG.error("kv flush WAL Failed.", e);
        }
    }

    public void put(final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws Exception {
        this.configRocksDBStorage.put(keyBytes, keyLen, valueBytes);
    }

    public void delete(final byte[] keyBytes) throws Exception {
        this.configRocksDBStorage.delete(keyBytes);
    }

    public void batchPutWithWal(final WriteBatch batch) throws Exception {
        this.configRocksDBStorage.batchPutWithWal(batch);
    }

    public Statistics getStatistics() {
        if (this.configRocksDBStorage == null) {
            return null;
        }

        return configRocksDBStorage.getStatistics();
    }
}
