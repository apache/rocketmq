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
package org.apache.rocketmq.broker;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;

public class RocksDBConfigManager {
    protected static final Logger BROKER_LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public volatile boolean isStop = false;
    public ConfigRocksDBStorage configRocksDBStorage = null;
    private FlushOptions flushOptions = null;
    private volatile long lastFlushMemTableMicroSecond = 0;
    private final String filePath;
    private final long memTableFlushInterval;
    private final CompressionType compressionType;
    private DataVersion kvDataVersion = new DataVersion();

    public RocksDBConfigManager(String filePath, long memTableFlushInterval, CompressionType compressionType) {
        this.filePath = filePath;
        this.memTableFlushInterval = memTableFlushInterval;
        this.compressionType = compressionType;
    }

    public boolean init() {
        this.isStop = false;
        this.configRocksDBStorage = new ConfigRocksDBStorage(filePath, compressionType);
        return this.configRocksDBStorage.start();
    }
    public boolean loadDataVersion() {
        String currDataVersionString = null;
        try {
            byte[] dataVersion = this.configRocksDBStorage.getKvDataVersion();
            if (dataVersion != null && dataVersion.length > 0) {
                currDataVersionString = new String(dataVersion, StandardCharsets.UTF_8);
            }
            kvDataVersion = StringUtils.isNotBlank(currDataVersionString) ? JSON.parseObject(currDataVersionString, DataVersion.class) : new DataVersion();
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean loadData(BiConsumer<byte[], byte[]> biConsumer) {
        try (RocksIterator iterator = this.configRocksDBStorage.iterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                biConsumer.accept(iterator.key(), iterator.value());
                iterator.next();
            }
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

    public void updateKvDataVersion() throws Exception {
        kvDataVersion.nextVersion();
        this.configRocksDBStorage.updateKvDataVersion(JSON.toJSONString(kvDataVersion).getBytes(StandardCharsets.UTF_8));
    }

    public DataVersion getKvDataVersion() {
        return kvDataVersion;
    }

    public void updateForbidden(String key, String value) throws Exception {
        this.configRocksDBStorage.updateForbidden(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
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
