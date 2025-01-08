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
package org.apache.rocketmq.broker.config.v1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.RocksDBConfigManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.rocksdb.CompressionType;
import org.rocksdb.WriteBatch;

public class RocksDBConsumerOffsetManager extends ConsumerOffsetManager {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected transient RocksDBConfigManager rocksDBConfigManager;

    public RocksDBConsumerOffsetManager(BrokerController brokerController) {
        super(brokerController);
        this.rocksDBConfigManager = new RocksDBConfigManager(rocksdbConfigFilePath(), brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs(),
            CompressionType.getCompressionType(brokerController.getMessageStoreConfig().getRocksdbCompressionType()));

    }

    @Override
    public boolean load() {
        if (!rocksDBConfigManager.init()) {
            return false;
        }
        if (!loadDataVersion() || !loadConsumerOffset()) {
            return false;
        }

        return true;
    }

    public boolean loadConsumerOffset() {
        return this.rocksDBConfigManager.loadData(this::decodeOffset) && merge();
    }

    private boolean merge() {
        if (!UtilAll.isPathExists(this.configFilePath()) && !UtilAll.isPathExists(this.configFilePath() + ".bak")) {
            log.info("consumerOffset json file does not exist, so skip merge");
            return true;
        }
        if (!super.loadDataVersion()) {
            log.error("load json consumerOffset dataVersion error, startup will exit");
            return false;
        }

        final DataVersion dataVersion = super.getDataVersion();
        final DataVersion kvDataVersion = this.getDataVersion();
        if (dataVersion.getCounter().get() > kvDataVersion.getCounter().get()) {
            if (!super.load()) {
                log.error("load json consumerOffset info failed, startup will exit");
                return false;
            }
            this.persist();
            this.getDataVersion().assignNewOne(dataVersion);
            updateDataVersion();
            log.info("update offset from json, dataVersion:{}, offsetTable: {} ", this.getDataVersion(), JSON.toJSONString(this.getOffsetTable()));
        }
        return true;
    }


    @Override
    public boolean stop() {
        return this.rocksDBConfigManager.stop();
    }

    @Override
    protected void removeConsumerOffset(String topicAtGroup) {
        try {
            byte[] keyBytes = topicAtGroup.getBytes(DataConverter.CHARSET_UTF8);
            this.rocksDBConfigManager.delete(keyBytes);
        } catch (Exception e) {
            LOG.error("kv remove consumerOffset Failed, {}", topicAtGroup);
        }
    }

    protected void decodeOffset(final byte[] key, final byte[] body) {
        String topicAtGroup = new String(key, DataConverter.CHARSET_UTF8);
        RocksDBOffsetSerializeWrapper wrapper = JSON.parseObject(body, RocksDBOffsetSerializeWrapper.class);

        this.offsetTable.put(topicAtGroup, wrapper.getOffsetTable());
        LOG.info("load exist local offset, {}, {}", topicAtGroup, wrapper.getOffsetTable());
    }

    public String rocksdbConfigFilePath() {
        return this.brokerController.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "consumerOffsets" + File.separator;
    }

    @Override
    public synchronized void persist() {
        WriteBatch writeBatch = new WriteBatch();
        try {
            Iterator<Entry<String, ConcurrentMap<Integer, Long>>> iterator = this.offsetTable.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, ConcurrentMap<Integer, Long>> entry = iterator.next();
                putWriteBatch(writeBatch, entry.getKey(), entry.getValue());

                if (writeBatch.getDataSize() >= 4 * 1024) {
                    this.rocksDBConfigManager.batchPutWithWal(writeBatch);
                }
            }
            this.rocksDBConfigManager.batchPutWithWal(writeBatch);
            this.rocksDBConfigManager.flushWAL();
        } catch (Exception e) {
            LOG.error("consumer offset persist Failed", e);
        } finally {
            writeBatch.close();
        }
    }

    public synchronized void exportToJson() {
        LOG.info("RocksDBConsumerOffsetManager export consumer offset to json file");
        super.persist();
    }

    private void putWriteBatch(final WriteBatch writeBatch, final String topicGroupName, final ConcurrentMap<Integer, Long> offsetMap) throws Exception {
        byte[] keyBytes = topicGroupName.getBytes(DataConverter.CHARSET_UTF8);
        RocksDBOffsetSerializeWrapper wrapper = new RocksDBOffsetSerializeWrapper();
        wrapper.setOffsetTable(offsetMap);
        byte[] valueBytes = JSON.toJSONBytes(wrapper, SerializerFeature.BrowserCompatible);
        writeBatch.put(keyBytes, valueBytes);
    }

    @Override
    public boolean loadDataVersion() {
        return this.rocksDBConfigManager.loadDataVersion();
    }

    @Override
    public DataVersion getDataVersion() {
        return rocksDBConfigManager.getKvDataVersion();
    }

    public void updateDataVersion() {
        try {
            rocksDBConfigManager.updateKvDataVersion();
        } catch (Exception e) {
            log.error("update consumer offset dataVersion error", e);
            throw new RuntimeException(e);
        }
    }
}
