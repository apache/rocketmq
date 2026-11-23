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
import com.alibaba.fastjson2.JSONWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.rocksdb.CompressionType;
import org.rocksdb.WriteBatch;

public class RocksDBConsumerOffsetManager extends ConsumerOffsetManager {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final String VERSION_COLUMN_FAMILY = "consumerOffsetVersion";
    private static final String OFFSET_COLUMN_FAMILY = "consumerOffset";
    private static final JSONWriter.Feature[] SERIALIZE_FEATURES = {JSONWriter.Feature.BrowserCompatible};
    private final transient AsyncOffsetPersister asyncPersister = new AsyncOffsetPersister();

    protected transient RocksDBConfigManager rocksDBConfigManager;
    private final boolean useSingleRocksDBForAllConfigs;
    private final String storePathRootDir;

    public RocksDBConsumerOffsetManager(BrokerController brokerController, boolean useSingleRocksDB,
        String storePathRootDir) {
        super(brokerController);

        this.useSingleRocksDBForAllConfigs = useSingleRocksDB;
        this.storePathRootDir = StringUtils.isBlank(storePathRootDir) ?
            brokerController.getMessageStoreConfig().getStorePathRootDir() : storePathRootDir;

        long flushInterval = brokerController.getMessageStoreConfig().getMemTableFlushIntervalMs();
        CompressionType compressionType =
            CompressionType.getCompressionType(brokerController.getMessageStoreConfig().getRocksdbCompressionType());
        String rocksDBPath = rocksdbConfigFilePath(storePathRootDir, useSingleRocksDB);

        this.rocksDBConfigManager = useSingleRocksDB ? new RocksDBConfigManager(rocksDBPath, flushInterval,
            compressionType, OFFSET_COLUMN_FAMILY, VERSION_COLUMN_FAMILY) : new RocksDBConfigManager(rocksDBPath,
            flushInterval, compressionType);
    }

    public RocksDBConsumerOffsetManager(BrokerController brokerController, boolean useSingleRocksDBForAllConfigs) {
        this(brokerController, useSingleRocksDBForAllConfigs, null);
    }

    public RocksDBConsumerOffsetManager(BrokerController brokerController) {
        this(brokerController, brokerController.getBrokerConfig().isUseSingleRocksDBForAllConfigs(), null);
    }

    @Override
    public synchronized boolean load() {
        if (!rocksDBConfigManager.init()) {
            return false;
        }
        if (!loadDataVersion() || !loadConsumerOffset()) {
            return false;
        }
        if (useSingleRocksDBForAllConfigs) {
            migrateFromSeparateRocksDBs();
        }
        asyncPersister.open();
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
            this.persist0(); // ensure full persistence
            this.getDataVersion().assignNewOne(dataVersion);
            updateDataVersion();
            log.info("update offset from json, dataVersion:{}, offsetTable: {} ", this.getDataVersion(), JSON.toJSONString(this.getOffsetTable()));
        }
        return true;
    }


    @Override
    public synchronized boolean stop() {
        return asyncPersister.close();
    }

    @Override
    public void removeConsumerOffset(String topicAtGroup) {
        try {
            asyncPersister.delete(topicAtGroup);
        } catch (Exception e) {
            log.error("kv remove consumerOffset Failed, {}", topicAtGroup);
        }
    }

    protected void decodeOffset(final byte[] key, final byte[] body) {
        String topicAtGroup = new String(key, DataConverter.CHARSET_UTF8);
        RocksDBOffsetSerializeWrapper wrapper = JSON.parseObject(body, RocksDBOffsetSerializeWrapper.class);

        this.offsetTable.put(topicAtGroup, wrapper.getOffsetTable());
        log.info("load exist local offset, {}, {}", topicAtGroup, wrapper.getOffsetTable());
    }

    public String rocksdbConfigFilePath(String storePathRootDir, boolean useSingleRocksDBForAllConfigs) {
        if (StringUtils.isBlank(storePathRootDir)) {
            storePathRootDir = brokerController.getMessageStoreConfig().getStorePathRootDir();
        }
        Path rootPath = Paths.get(storePathRootDir);
        if (useSingleRocksDBForAllConfigs) {
            return rootPath.resolve("config").resolve("metadata").toString();
        }
        return rootPath.resolve("config").resolve("consumerOffsets").toString();
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.storePathRootDir);
    }

    @Override
    public synchronized void persist() {
        try {
            asyncPersister.persist();
        } catch (Exception e) {
            log.error("consumer offset persist Failed", e);
        }
    }

    private void persist0() {
        if (rocksDBConfigManager.isLoaded()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                RocksDBOffsetSerializeWrapper wrapper = new RocksDBOffsetSerializeWrapper();
                for (Entry<String, ConcurrentMap<Integer, Long>> entry : this.offsetTable.entrySet()) {
                    putWriteBatch(writeBatch, entry.getKey(), entry.getValue(), wrapper);
                    if (writeBatch.getDataSize() >= 4 * 1024) {
                        this.rocksDBConfigManager.batchPutWithWal(writeBatch);
                    }
                }
                this.rocksDBConfigManager.batchPutWithWal(writeBatch);
                this.rocksDBConfigManager.persistKvDataVersion();
                this.rocksDBConfigManager.flushWAL();
            } catch (Exception e) {
                log.error("consumer offset persist Failed", e);
            }
        } else {
            log.warn("RocksDBConsumerOffsetManager has been stopped, persist fail");
        }
    }

    @Override
    public void commitOffset(String clientHost, String group, String topic, int queueId, long offset) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        updateOffsetInMemory(clientHost, key, topic, queueId, offset);
        if (brokerController.getBrokerConfig().isPersistConsumerOffsetIncrementally()) {
            asyncPersister.commit(key);
        }
        updateDataVersionByStep();
    }

    private void updateOffsetInMemory(String clientHost, String key, String topic, int queueId, long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = MixAll.isLmq(topic) ? new ConcurrentHashMap<>(1, 1.0F) : new ConcurrentHashMap<>();
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                LOG.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    private void updateDataVersionByStep() {
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getConsumerOffsetUpdateVersionStep() == 0) {
            getDataVersion().nextVersion();
        }
    }

    public synchronized void exportToJson() {
        log.info("RocksDBConsumerOffsetManager export consumer offset to json file");
        super.persist();
    }

    private void putWriteBatch(final WriteBatch writeBatch, final String topicGroupName,
        final ConcurrentMap<Integer, Long> offsetMap, final RocksDBOffsetSerializeWrapper wrapper) throws Exception {
        byte[] keyBytes = topicGroupName.getBytes(DataConverter.CHARSET_UTF8);
        wrapper.setOffsetTable(offsetMap);
        byte[] valueBytes = JSON.toJSONBytes(wrapper, SERIALIZE_FEATURES);
        rocksDBConfigManager.writeBatchPutOperation(writeBatch, keyBytes, valueBytes);
    }

    @Override
    public boolean loadDataVersion() {
        return this.rocksDBConfigManager.loadDataVersion();
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
            log.error("update consumer offset dataVersion error", e);
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
        if (!UtilAll.isPathExists(separateRocksDBPath)) {
            log.info("Separate RocksDB for consumer offsets does not exist at {}, no migration needed",
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

            DataVersion separateDataVersion = separateRocksDBConfigManager.getKvDataVersion();
            DataVersion unifiedDataVersion = this.getDataVersion();

            log.info("Comparing data versions - Separate: {}, Unified: {}", separateDataVersion, unifiedDataVersion);

            // Compare versions and import if separate version is newer
            if (separateDataVersion.getCounter().get() > unifiedDataVersion.getCounter().get()) {
                log.info("Separate RocksDB has newer data, importing...");

                // Load consumer offsets from separate RocksDB
                if (separateRocksDBConfigManager.loadData(this::importConsumerOffset)) {
                    log.info("Successfully imported consumer offsets from separate RocksDB");

                    // Update unified data version to be newer than separate one
                    this.getDataVersion().assignNewOne(separateDataVersion);
                    this.getDataVersion().nextVersion(); // Make it one version higher
                    updateDataVersion();

                    log.info("Updated unified data version to {}", this.getDataVersion());
                } else {
                    log.error("Failed to import consumer offsets from separate RocksDB");
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
     * Import a consumer offset from the separate RocksDB during migration
     *
     * @param key  The topic@group name bytes
     * @param body The consumer offset data bytes
     */
    private void importConsumerOffset(final byte[] key, final byte[] body) {
        try {
            decodeOffset(key, body);
            this.rocksDBConfigManager.put(key, body);
        } catch (Exception e) {
            log.error("Error importing consumer offset", e);
        }
    }

    // Offset updates and version semantics belong to the manager; queuing, scheduling and write coordination are centralized in this component.
    private final class AsyncOffsetPersister {
        private static final int BATCH_KEYS = 500;
        private static final int BATCH_BYTES = 256 * 1024;
        private static final long INTERVAL_MS = 20;
        private static final long STOP_TIMEOUT_SECONDS = 30;

        // Only the writeLock -> pendingLock ordering is allowed; the registration path never acquires writeLock.
        private final Object pendingLock = new Object();
        private final Object writeLock = new Object();
        // Deduplicated in registration order; duplicate updates keep their original position and only mutate in-memory state.
        private final Set<String> pendingKeys = new LinkedHashSet<>();
        // Guarded by pendingLock; non-null means running. Set to null on close, after which commits are kept in memory only.
        private ScheduledExecutorService executor;

        private void open() {
            synchronized (pendingLock) {
                if (executor != null) {
                    return;
                }
                executor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("AsyncOffsetPersist_", true));
                executor.scheduleWithFixedDelay(() -> {
                    try {
                        flushBatch();
                    } catch (Exception e) {
                        log.error("Failed to persist offset batch", e);
                    }
                }, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
            }
        }

        private void commit(String key) {
            synchronized (pendingLock) {
                if (executor == null) {
                    log.warn("Async offset persistence is stopped, offset only kept in memory, key={}", key);
                    return;
                }
                pendingKeys.add(key);
            }
        }

        private void delete(String key) throws Exception {
            synchronized (writeLock) {
                synchronized (pendingLock) {
                    pendingKeys.remove(key);
                }
                rocksDBConfigManager.delete(key.getBytes(DataConverter.CHARSET_UTF8));
            }
        }

        private void persist() throws Exception {
            synchronized (writeLock) {
                flushPending();
                if (brokerController.getBrokerConfig().isPersistConsumerOffsetIncrementally()) {
                    updateDataVersion();
                    rocksDBConfigManager.flushWALChecked();
                } else {
                    persist0();
                }
            }
        }

        private boolean close() {
            ScheduledExecutorService running;
            synchronized (pendingLock) {
                running = executor;
                executor = null;
            }
            if (running != null) {
                ThreadUtils.shutdownGracefully(running, STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!running.isTerminated()) {
                    return false;
                }
            }
            synchronized (writeLock) {
                try {
                    flushPending();
                    if (running != null) {
                        rocksDBConfigManager.persistKvDataVersion();
                        rocksDBConfigManager.flushWALChecked();
                    }
                } catch (Exception e) {
                    log.error("Failed to stop async offset persistence, keep RocksDB open", e);
                    return false;
                }
                return rocksDBConfigManager.stop();
            }
        }

        private void flushBatch() throws Exception {
            synchronized (pendingLock) {
                if (pendingKeys.isEmpty()) {
                    return;
                }
            }
            synchronized (writeLock) {
                flushBatchLocked(BATCH_KEYS);
            }
        }

        // Callers hold writeLock, so the batch-write core does not re-acquire the write lock.
        private int flushBatchLocked(int limit) throws Exception {
            List<String> selected = new ArrayList<>(limit);
            try (WriteBatch batch = new WriteBatch()) {
                RocksDBOffsetSerializeWrapper wrapper = new RocksDBOffsetSerializeWrapper();
                while (selected.size() < limit && batch.getDataSize() < BATCH_BYTES) {
                    String key;
                    synchronized (pendingLock) {
                        Iterator<String> it = pendingKeys.iterator();
                        if (!it.hasNext()) {
                            break;
                        }
                        // Take the earliest registered key; remove the mark before reading memory, later updates will re-register it.
                        key = it.next();
                        it.remove();
                        selected.add(key);
                    }
                    ConcurrentMap<Integer, Long> offsets = offsetTable.get(key);
                    if (offsets == null) {
                        continue;
                    }
                    putWriteBatch(batch, key, offsets, wrapper);
                }
                if (batch.count() > 0) {
                    rocksDBConfigManager.batchPutWithWal(batch);
                }
                return selected.size();
            } catch (Exception e) {
                synchronized (pendingLock) {
                    pendingKeys.addAll(selected);
                }
                throw e;
            }
        }

        // Callers hold writeLock; only drains the current count, new commits are handled by the next round.
        private void flushPending() throws Exception {
            int remaining;
            synchronized (pendingLock) {
                remaining = pendingKeys.size();
            }
            while (remaining > 0) {
                remaining -= flushBatchLocked(Math.min(remaining, BATCH_KEYS));
            }
        }
    }
}
