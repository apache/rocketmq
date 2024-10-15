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
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.RocksDBConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.rocksdb.CompressionType;

public class RocksDBTopicConfigManager extends TopicConfigManager {

    protected RocksDBConfigManager rocksDBConfigManager;

    public RocksDBTopicConfigManager(BrokerController brokerController) {
        super(brokerController, false);
        this.rocksDBConfigManager = new RocksDBConfigManager(rocksdbConfigFilePath(), brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs(),
            CompressionType.getCompressionType(brokerController.getMessageStoreConfig().getRocksdbCompressionType()));
    }

    @Override
    public boolean load() {
        if (!rocksDBConfigManager.init()) {
            return false;
        }
        if (!loadDataVersion() || !loadTopicConfig()) {
            return false;
        }
        this.init();
        return true;
    }

    public boolean loadTopicConfig() {
        return this.rocksDBConfigManager.loadData(this::decodeTopicConfig) && merge();
    }

    public boolean loadDataVersion() {
        return this.rocksDBConfigManager.loadDataVersion();
    }

    private boolean merge() {
        if (!UtilAll.isPathExists(this.configFilePath()) && !UtilAll.isPathExists(this.configFilePath() + ".bak")) {
            log.info("topic json file does not exist, so skip merge");
            return true;
        }

        if (!super.loadDataVersion()) {
            log.error("load json topic dataVersion error, startup will exit");
            return false;
        }

        final DataVersion dataVersion = super.getDataVersion();
        final DataVersion kvDataVersion = this.getDataVersion();
        if (dataVersion.getCounter().get() > kvDataVersion.getCounter().get()) {
            if (!super.load()) {
                log.error("load topic config from json file error, startup will exit");
                return false;
            }
            final ConcurrentMap<String, TopicConfig> topicConfigTable = this.getTopicConfigTable();
            for (Map.Entry<String, TopicConfig> entry : topicConfigTable.entrySet()) {
                putTopicConfig(entry.getValue());
                log.info("import topic config to rocksdb, topic={}", entry.getValue());
            }
            this.getDataVersion().assignNewOne(dataVersion);
            updateDataVersion();
        } else {
            log.info("dataVersion is not greater than kvDataVersion, no need to merge topic metaData, dataVersion={}, kvDataVersion={}", dataVersion, kvDataVersion);
        }
        log.info("finish read topic config from json file and merge to rocksdb");
        this.persist();
        return true;
    }


    @Override
    public boolean stop() {
        return this.rocksDBConfigManager.stop();
    }

    protected void decodeTopicConfig(byte[] key, byte[] body) {
        String topicName = new String(key, DataConverter.CHARSET_UTF8);
        TopicConfig topicConfig = JSON.parseObject(body, TopicConfig.class);

        this.topicConfigTable.put(topicName, topicConfig);
        log.info("load exist local topic, {}", topicConfig.toString());
    }

    @Override
    public TopicConfig putTopicConfig(TopicConfig topicConfig) {
        String topicName = topicConfig.getTopicName();
        TopicConfig oldTopicConfig = this.topicConfigTable.put(topicName, topicConfig);
        try {
            byte[] keyBytes = topicName.getBytes(DataConverter.CHARSET_UTF8);
            byte[] valueBytes = JSON.toJSONBytes(topicConfig, SerializerFeature.BrowserCompatible);
            this.rocksDBConfigManager.put(keyBytes, keyBytes.length, valueBytes);
        } catch (Exception e) {
            log.error("kv put topic Failed, {}", topicConfig.toString(), e);
        }
        return oldTopicConfig;
    }

    @Override
    protected TopicConfig removeTopicConfig(String topicName) {
        TopicConfig topicConfig = this.topicConfigTable.remove(topicName);
        try {
            this.rocksDBConfigManager.delete(topicName.getBytes(DataConverter.CHARSET_UTF8));
        } catch (Exception e) {
            log.error("kv remove topic Failed, {}", topicConfig.toString());
        }
        return topicConfig;
    }

    @Override
    public synchronized void persist() {
        if (brokerController.getMessageStoreConfig().isRealTimePersistRocksDBConfig()) {
            this.rocksDBConfigManager.flushWAL();
        }
    }

    public String rocksdbConfigFilePath() {
        return this.brokerController.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "topics" + File.separator;
    }


    @Override
    public DataVersion getDataVersion() {
        return rocksDBConfigManager.getKvDataVersion();
    }

    @Override
    public void updateDataVersion() {
        try {
            rocksDBConfigManager.updateKvDataVersion();
        } catch (Exception e) {
            log.error("update topic config dataVersion error", e);
            throw new RuntimeException(e);
        }
    }
}
