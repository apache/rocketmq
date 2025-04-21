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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.RocksDBConfigManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class RocksDBSubscriptionGroupManager extends SubscriptionGroupManager {

    protected transient RocksDBConfigManager rocksDBConfigManager;

    public RocksDBSubscriptionGroupManager(BrokerController brokerController) {
        super(brokerController, false);
        this.rocksDBConfigManager = new RocksDBConfigManager(rocksdbConfigFilePath(), brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs(),
            CompressionType.getCompressionType(brokerController.getMessageStoreConfig().getRocksdbCompressionType()));
    }

    @Override
    public boolean load() {
        if (!rocksDBConfigManager.init()) {
            return false;
        }
        if (!loadDataVersion() || !loadSubscriptionGroupAndForbidden()) {
            return false;
        }
        this.init();
        return true;
    }

    public boolean loadDataVersion() {
        return this.rocksDBConfigManager.loadDataVersion();
    }

    public boolean loadSubscriptionGroupAndForbidden() {
        return this.rocksDBConfigManager.loadData(this::decodeSubscriptionGroup)
                && this.loadForbidden(this::decodeForbidden)
                && merge();
    }

    public boolean loadForbidden(BiConsumer<byte[], byte[]> biConsumer) {
        try (RocksIterator iterator = this.rocksDBConfigManager.configRocksDBStorage.forbiddenIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                biConsumer.accept(iterator.key(), iterator.value());
                iterator.next();
            }
        }
        return true;
    }

    private boolean merge() {
        if (!UtilAll.isPathExists(this.configFilePath()) && !UtilAll.isPathExists(this.configFilePath() + ".bak")) {
            log.info("subGroup json file does not exist, so skip merge");
            return true;
        }
        if (!super.loadDataVersion()) {
            log.error("load json subGroup dataVersion error, startup will exit");
            return false;
        }
        final DataVersion dataVersion = super.getDataVersion();
        final DataVersion kvDataVersion = this.getDataVersion();
        if (dataVersion.getCounter().get() > kvDataVersion.getCounter().get()) {
            if (!super.load()) {
                log.error("load group and forbidden info from json file error, startup will exit");
                return false;
            }
            final ConcurrentMap<String, SubscriptionGroupConfig> groupTable = this.getSubscriptionGroupTable();
            for (Map.Entry<String, SubscriptionGroupConfig> entry : groupTable.entrySet()) {
                putSubscriptionGroupConfig(entry.getValue());
                log.info("import subscription config to rocksdb, group={}", entry.getValue());
            }
            final ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable = this.getForbiddenTable();
            for (Map.Entry<String, ConcurrentMap<String, Integer>> entry : forbiddenTable.entrySet()) {
                try {
                    this.rocksDBConfigManager.updateForbidden(entry.getKey(), JSON.toJSONString(entry.getValue()));
                    log.info("import forbidden config to rocksdb, group={}", entry.getValue());
                } catch (Exception e) {
                    log.error("import forbidden config to rocksdb failed, group={}", entry.getValue());
                    return false;
                }
            }
            this.getDataVersion().assignNewOne(dataVersion);
            updateDataVersion();
        } else {
            log.info("dataVersion is not greater than kvDataVersion, no need to merge group metaData, dataVersion={}, kvDataVersion={}", dataVersion, kvDataVersion);
        }
        log.info("finish marge subscription config from json file and merge to rocksdb");
        this.persist();

        return true;
    }

    @Override
    public boolean stop() {
        return this.rocksDBConfigManager.stop();
    }

    @Override
    public SubscriptionGroupConfig putSubscriptionGroupConfig(SubscriptionGroupConfig subscriptionGroupConfig) {
        String groupName = subscriptionGroupConfig.getGroupName();
        SubscriptionGroupConfig oldConfig = this.subscriptionGroupTable.put(groupName, subscriptionGroupConfig);

        try {
            byte[] keyBytes = groupName.getBytes(DataConverter.CHARSET_UTF8);
            byte[] valueBytes = JSON.toJSONBytes(subscriptionGroupConfig, JSONWriter.Feature.BrowserCompatible);
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
                byte[] keyBytes = groupName.getBytes(DataConverter.CHARSET_UTF8);
                byte[] valueBytes = JSON.toJSONBytes(subscriptionGroupConfig, JSONWriter.Feature.BrowserCompatible);
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
            this.rocksDBConfigManager.delete(groupName.getBytes(DataConverter.CHARSET_UTF8));
        } catch (Exception e) {
            log.error("kv delete sub Failed, {}", subscriptionGroupConfig.toString());
        }
        return subscriptionGroupConfig;
    }


    protected void decodeSubscriptionGroup(byte[] key, byte[] body) {
        String groupName = new String(key, DataConverter.CHARSET_UTF8);
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

    public synchronized void exportToJson() {
        log.info("RocksDBSubscriptionGroupManager export subscription group to json file");
        super.persist();
    }

    public String rocksdbConfigFilePath() {
        return this.brokerController.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "subscriptionGroups" + File.separator;
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
            log.error("update group config dataVersion error", e);
            throw new RuntimeException(e);
        }
    }

    protected void decodeForbidden(byte[] key, byte[] body) {
        String forbiddenGroupName = new String(key, DataConverter.CHARSET_UTF8);
        JSONObject jsonObject = JSON.parseObject(new String(body, DataConverter.CHARSET_UTF8));
        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
        ConcurrentMap<String, Integer> forbiddenGroup = new ConcurrentHashMap<>(entries.size());
        for (Map.Entry<String, Object> entry : entries) {
            forbiddenGroup.put(entry.getKey(), (Integer) entry.getValue());
        }
        this.getForbiddenTable().put(forbiddenGroupName, forbiddenGroup);
        log.info("load forbidden,{} value {}", forbiddenGroupName, forbiddenGroup.toString());
    }

    @Override
    public void updateForbidden(String group, String topic, int forbiddenIndex, boolean setOrClear) {
        try {
            super.updateForbidden(group, topic, forbiddenIndex, setOrClear);
            this.rocksDBConfigManager.updateForbidden(group, JSON.toJSONString(this.getForbiddenTable().get(group)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setForbidden(String group, String topic, int forbiddenIndex) {
        try {
            super.setForbidden(group, topic, forbiddenIndex);
            this.rocksDBConfigManager.updateForbidden(group, JSON.toJSONString(this.getForbiddenTable().get(group)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearForbidden(String group, String topic, int forbiddenIndex) {
        try {
            super.clearForbidden(group, topic, forbiddenIndex);
            this.rocksDBConfigManager.updateForbidden(group, JSON.toJSONString(this.getForbiddenTable().get(group)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
