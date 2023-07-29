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
package org.apache.rocketmq.broker.subscription;

import java.io.File;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class RocksDBSubscriptionGroupManager extends SubscriptionGroupManager {

    public RocksDBSubscriptionGroupManager(BrokerController brokerController) {
        super(brokerController, false);
        this.rocksDBConfigManager = new RocksDBConfigManager(this.brokerController.getMessageStoreConfig().getMemTableFlushInterval());
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
    protected SubscriptionGroupConfig putSubscriptionGroupConfig(SubscriptionGroupConfig subscriptionGroupConfig) {
        String groupName = subscriptionGroupConfig.getGroupName();
        SubscriptionGroupConfig oldConfig = this.subscriptionGroupTable.put(groupName, subscriptionGroupConfig);

        try {
            byte[] keyBytes = groupName.getBytes(DataConverter.charset);
            byte[] valueBytes = JSON.toJSONBytes(subscriptionGroupConfig, SerializerFeature.BrowserCompatible);
            this.rocksDBConfigManager.put(keyBytes, keyBytes.length, valueBytes);
        } catch (Exception e) {
            log.error("kv put sub Failed, {}", subscriptionGroupConfig.toString());
        }
        return oldConfig;
    }

    @Override
    protected SubscriptionGroupConfig putSubscriptionGroupConfigIfAbsent(SubscriptionGroupConfig subscriptionGroupConfig) {
        String groupName = subscriptionGroupConfig.getGroupName();
        SubscriptionGroupConfig oldConfig = this.subscriptionGroupTable.putIfAbsent(groupName, subscriptionGroupConfig);
        if (oldConfig == null) {
            try {
                byte[] keyBytes = groupName.getBytes(DataConverter.charset);
                byte[] valueBytes = JSON.toJSONBytes(subscriptionGroupConfig, SerializerFeature.BrowserCompatible);
                this.rocksDBConfigManager.put(keyBytes, keyBytes.length, valueBytes);
            } catch (Exception e) {
                log.error("kv put sub Failed, {}", subscriptionGroupConfig.toString());
            }
        }
        return oldConfig;
    }

    @Override
    protected SubscriptionGroupConfig removeSubscriptionGroupConfig(String groupName) {
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.remove(groupName);
        try {
            this.rocksDBConfigManager.delete(groupName.getBytes(DataConverter.charset));
        } catch (Exception e) {
            log.error("kv delete sub Failed, {}", subscriptionGroupConfig.toString());
        }
        return subscriptionGroupConfig;
    }

    @Override
    protected void decode0(byte[] key, byte[] body) {
        String groupName = new String(key, DataConverter.charset);
        SubscriptionGroupConfig subscriptionGroupConfig = JSON.parseObject(body, SubscriptionGroupConfig.class);

        this.subscriptionGroupTable.put(groupName, subscriptionGroupConfig);
        log.info("load exist local sub, {}", subscriptionGroupConfig.toString());
    }

    @Override
    public synchronized void persist() {
        if (brokerController.getMessageStoreConfig().isRealTimePersistRocksDBConfig()) {
            this.rocksDBConfigManager.flushWAL();
        }
    }

    @Override
    public String configFilePath() {
        return this.brokerController.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "subscriptionGroups" + File.separator;
    }
}
