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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.rocksdb.CompressionType;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class RocksDBSubscriptionGroupManager extends SubscriptionGroupManager {

    protected transient RocksDBConfigManager rocksDBConfigManager;

    private static final String VERSION_COLUMN_FAMILY = "subscriptionGroupVersion";
    private static final String GROUP_COLUMN_FAMILY = "subscriptionGroup";
    private static final String FORBIDDEN_COLUMN_FAMILY_NAME = "forbidden";

    private final boolean useSingleRocksDBForAllConfigs;
    private final String storePathRootDir;

    public RocksDBSubscriptionGroupManager(BrokerController brokerController, boolean useSingleRocksDB,
        String storePathRootDir) {
        super(brokerController, false);

        this.useSingleRocksDBForAllConfigs = useSingleRocksDB;
        this.storePathRootDir = StringUtils.isBlank(storePathRootDir) ?
            brokerController.getMessageStoreConfig().getStorePathRootDir() : storePathRootDir;

        long flushInterval = brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs();
        CompressionType compressionType =
            CompressionType.getCompressionType(brokerController.getMessageStoreConfig().getRocksdbCompressionType());
        String rocksDBPath = rocksdbConfigFilePath(storePathRootDir, useSingleRocksDB);

        this.rocksDBConfigManager = useSingleRocksDB ? new RocksDBConfigManager(rocksDBPath, flushInterval,
            compressionType, GROUP_COLUMN_FAMILY, VERSION_COLUMN_FAMILY) : new RocksDBConfigManager(rocksDBPath,
            flushInterval, compressionType);
    }

    public RocksDBSubscriptionGroupManager(BrokerController brokerController, boolean useSingleRocksDBForAllConfigs) {
        this(brokerController, useSingleRocksDBForAllConfigs, null);
    }

    public RocksDBSubscriptionGroupManager(BrokerController brokerController) {
        this(brokerController, brokerController.getBrokerConfig().isUseSingleRocksDBForAllConfigs(), null);
    }

    @Override
    public boolean load() {
        if (!rocksDBConfigManager.init()) {
            return false;
        }
        if (!loadDataVersion() || !loadSubscriptionGroupAndForbidden()) {
            return false;
        }
        if (useSingleRocksDBForAllConfigs) {
            migrateFromSeparateRocksDBs();
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
        try {
            this.rocksDBConfigManager.configRocksDBStorage.iterate(FORBIDDEN_COLUMN_FAMILY_NAME, biConsumer);
            return true;
        } catch (Exception e) {
            log.error("loadForbidden exception", e);
        }
        return false;
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
                    this.rocksDBConfigManager.put(FORBIDDEN_COLUMN_FAMILY_NAME, entry.getKey(),
                        JSON.toJSONString(entry.getValue()));
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
        log.info("finish merge subscription config from json file and merge to rocksdb");
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
            byte[] keyBytes = groupName.getBytes(RocksDBConfigManager.CHARSET);
            byte[] valueBytes = JSON.toJSONBytes(subscriptionGroupConfig, JSONWriter.Feature.BrowserCompatible);
            this.rocksDBConfigManager.put(keyBytes, valueBytes);
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
                byte[] keyBytes = groupName.getBytes(RocksDBConfigManager.CHARSET);
                byte[] valueBytes = JSON.toJSONBytes(subscriptionGroupConfig, JSONWriter.Feature.BrowserCompatible);
                this.rocksDBConfigManager.put(keyBytes, valueBytes);
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
            this.rocksDBConfigManager.delete(groupName.getBytes(RocksDBConfigManager.CHARSET));
        } catch (Exception e) {
            log.error("kv delete sub Failed, {}", subscriptionGroupConfig.toString());
        }
        return subscriptionGroupConfig;
    }


    protected void decodeSubscriptionGroup(byte[] key, byte[] body) {
        String groupName = new String(key, RocksDBConfigManager.CHARSET);
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

    public String rocksdbConfigFilePath(String storePathRootDir, boolean useSingleRocksDBForAllConfigs) {
        if (StringUtils.isBlank(storePathRootDir)) {
            storePathRootDir = brokerController.getMessageStoreConfig().getStorePathRootDir();
        }
        Path rootPath = Paths.get(storePathRootDir);
        if (useSingleRocksDBForAllConfigs) {
            return rootPath.resolve("config").resolve("metadata").toString();
        }
        return rootPath.resolve("config").resolve("subscriptionGroups").toString();
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.storePathRootDir);
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
        String forbiddenGroupName = new String(key, RocksDBConfigManager.CHARSET);
        JSONObject jsonObject = JSON.parseObject(new String(body, RocksDBConfigManager.CHARSET));
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
            this.rocksDBConfigManager.put(FORBIDDEN_COLUMN_FAMILY_NAME, group,
                JSON.toJSONString(this.getForbiddenTable().get(group)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setForbidden(String group, String topic, int forbiddenIndex) {
        try {
            super.setForbidden(group, topic, forbiddenIndex);
            this.rocksDBConfigManager.put(FORBIDDEN_COLUMN_FAMILY_NAME, group,
                JSON.toJSONString(this.getForbiddenTable().get(group)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearForbidden(String group, String topic, int forbiddenIndex) {
        try {
            super.clearForbidden(group, topic, forbiddenIndex);
            this.rocksDBConfigManager.put(FORBIDDEN_COLUMN_FAMILY_NAME, group,
                JSON.toJSONString(this.getForbiddenTable().get(group)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Migrate data from separate RocksDB instances to the unified RocksDB when useSingleRocksDBForAllConfigs is
     * enabled.
     * This method will only be called when switching from separate RocksDB mode to unified mode.
     * It opens the separate RocksDB in read-only mode, compares versions, and imports data if needed.
     */
    private void migrateFromSeparateRocksDBs() {
        String separateRocksDBPath = rocksdbConfigFilePath(this.storePathRootDir, false);

        // Check if separate RocksDB exists
        if (!org.apache.rocketmq.common.UtilAll.isPathExists(separateRocksDBPath)) {
            log.info("Separate RocksDB for subscription groups does not exist at {}, no migration needed",
                separateRocksDBPath);
            return;
        }

        log.info("Starting migration from separate RocksDB at {} to unified RocksDB", separateRocksDBPath);

        // Open separate RocksDB in read-only mode
        RocksDBConfigManager separateRocksDBConfigManager = null;
        try {
            long memTableFlushIntervalMs = brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs();
            org.rocksdb.CompressionType compressionType =
                org.rocksdb.CompressionType.getCompressionType(brokerController.getMessageStoreConfig().getRocksdbCompressionType());

            separateRocksDBConfigManager = new RocksDBConfigManager(separateRocksDBPath, memTableFlushIntervalMs,
                compressionType);

            // Initialize in read-only mode
            if (!separateRocksDBConfigManager.init(true)) {
                log.error("Failed to initialize separate RocksDB in read-only mode");
                return;
            }

            // Load data version from separate RocksDB
            if (!separateRocksDBConfigManager.loadDataVersion()) {
                log.error("Failed to load data version from separate RocksDB");
                return;
            }

            org.apache.rocketmq.remoting.protocol.DataVersion separateDataVersion =
                separateRocksDBConfigManager.getKvDataVersion();
            org.apache.rocketmq.remoting.protocol.DataVersion unifiedDataVersion = this.getDataVersion();

            log.info("Comparing data versions - Separate: {}, Unified: {}", separateDataVersion, unifiedDataVersion);

            // Compare versions and import if separate version is newer
            if (separateDataVersion.getCounter().get() > unifiedDataVersion.getCounter().get()) {
                log.info("Separate RocksDB has newer data, importing...");

                // Load subscription groups from separate RocksDB
                boolean success = separateRocksDBConfigManager.loadData(this::importSubscriptionGroup);
                if (success) {
                    // Load forbidden data directly using the storage
                    try {
                        separateRocksDBConfigManager.configRocksDBStorage.iterate(FORBIDDEN_COLUMN_FAMILY_NAME,
                            this::importForbidden);
                        log.info("Successfully imported subscription groups and forbidden data from separate RocksDB");

                        // Update unified data version to be newer than separate one
                        this.getDataVersion().assignNewOne(separateDataVersion);
                        this.getDataVersion().nextVersion(); // Make it one version higher
                        updateDataVersion();

                        log.info("Updated unified data version to {}", this.getDataVersion());
                    } catch (Exception e) {
                        log.error("Failed to import forbidden data from separate RocksDB", e);
                        success = false;
                    }
                }

                if (!success) {
                    log.error("Failed to import subscription groups or forbidden data from separate RocksDB");
                }
            } else {
                log.info("Unified RocksDB is already up-to-date, no migration needed");
            }
        } catch (Exception e) {
            log.error("Error during migration from separate RocksDB", e);
        } finally {
            // Clean up resources
            if (separateRocksDBConfigManager != null) {
                try {
                    separateRocksDBConfigManager.stop();
                } catch (Exception e) {
                    log.warn("Error stopping separate RocksDB config manager", e);
                }
            }
        }
    }

    /**
     * Import a subscription group from the separate RocksDB during migration
     *
     * @param key  The group name bytes
     * @param body The subscription group data bytes
     */
    private void importSubscriptionGroup(byte[] key, byte[] body) {
        try {
            decodeSubscriptionGroup(key, body);
            this.rocksDBConfigManager.put(key, body);
        } catch (Exception e) {
            log.error("Error importing subscription group", e);
        }
    }

    /**
     * Import forbidden data from the separate RocksDB during migration
     *
     * @param key  The group name bytes
     * @param body The forbidden data bytes
     */
    private void importForbidden(byte[] key, byte[] body) {
        try {
            decodeForbidden(key, body);
            this.rocksDBConfigManager.put(FORBIDDEN_COLUMN_FAMILY_NAME, key, body);
        } catch (Exception e) {
            log.error("Error importing forbidden data", e);
        }
    }
}
