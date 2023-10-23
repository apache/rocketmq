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
package org.apache.rocketmq.broker.topic;

import java.io.File;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.common.utils.DataConverter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class RocksDBTopicConfigManager extends TopicConfigManager {

    public RocksDBTopicConfigManager(BrokerController brokerController) {
        super(brokerController, false);
        this.rocksDBConfigManager = new RocksDBConfigManager(brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs());
    }

    @Override
    public boolean load() {
        if (!this.rocksDBConfigManager.load(configFilePath(), this::decode0)) {
            return false;
        }
        this.init();
        return true;
    }

    @Override
    public boolean stop() {
        return this.rocksDBConfigManager.stop();
    }

    @Override
    protected void decode0(byte[] key, byte[] body) {
        String topicName = new String(key, DataConverter.CHARSET_UTF8);
        TopicConfig topicConfig = JSON.parseObject(body, TopicConfig.class);

        this.topicConfigTable.put(topicName, topicConfig);
        log.info("load exist local topic, {}", topicConfig.toString());
    }

    @Override
    public String configFilePath() {
        return this.brokerController.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "topics" + File.separator;
    }

    @Override
    protected TopicConfig putTopicConfig(TopicConfig topicConfig) {
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
}
