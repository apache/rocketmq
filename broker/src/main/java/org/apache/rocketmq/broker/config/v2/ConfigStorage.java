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
package org.apache.rocketmq.broker.config.v2;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.config.ConfigHelper;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * https://book.tidb.io/session1/chapter3/tidb-kv-to-relation.html
 */
public class ConfigStorage extends AbstractRocksDBStorage {

    public static final String DATA_VERSION_KEY = "data_version";
    public static final byte[] DATA_VERSION_KEY_BYTES = DATA_VERSION_KEY.getBytes(StandardCharsets.UTF_8);

    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Number of write ops since previous flush.
     */
    private final AtomicInteger writeOpsCounter;

    private final MessageStoreConfig messageStoreConfig;

    public ConfigStorage(MessageStoreConfig messageStoreConfig) {
        super(messageStoreConfig.getStorePathRootDir() + File.separator + "config" + File.separator + "rdb");
        this.messageStoreConfig = messageStoreConfig;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("config-storage-%d")
            .build();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(2, threadFactory);
        writeOpsCounter = new AtomicInteger(0);
    }

    private void statNettyMemory() {
        PooledByteBufAllocatorMetric metric = AbstractRocksDBStorage.POOLED_ALLOCATOR.metric();
        LOGGER.info("Netty Memory Usage: {}", metric);
    }

    @Override
    public synchronized boolean start() {
        boolean started = super.start();
        if (started) {
            scheduledExecutorService.scheduleWithFixedDelay(() -> statRocksdb(LOGGER), 1, 10, TimeUnit.SECONDS);
            scheduledExecutorService.scheduleWithFixedDelay(this::statNettyMemory, 10, 10, TimeUnit.SECONDS);
        } else {
            LOGGER.error("Failed to start config storage");
        }
        return started;
    }

    @Override
    protected boolean postLoad() {
        if (!PlatformDependent.hasUnsafe()) {
            LOGGER.error("Unsafe not available and POOLED_ALLOCATOR cannot work correctly");
            return false;
        }
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

            ColumnFamilyOptions defaultOptions = ConfigHelper.createConfigColumnFamilyOptions();
            this.cfOptions.add(defaultOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));

            // Start RocksDB instance
            open(cfDescriptors);

            this.defaultCFHandle = cfHandles.get(0);
        } catch (Exception e) {
            AbstractRocksDBStorage.LOGGER.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {
        scheduledExecutorService.shutdown();
    }

    protected void initOptions() {
        this.options = ConfigHelper.createConfigDBOptions();
        super.initOptions();
    }

    @Override
    protected void initAbleWalWriteOptions() {
        this.ableWalWriteOptions = new WriteOptions();

        // Given that fdatasync is kind of expensive, sync-WAL for every write cannot be afforded.
        this.ableWalWriteOptions.setSync(false);

        // We need WAL for config changes
        this.ableWalWriteOptions.setDisableWAL(false);

        // No fast failure on block, wait synchronously even if there is wait for the write request
        this.ableWalWriteOptions.setNoSlowdown(false);
    }

    public byte[] get(ByteBuffer key) throws RocksDBException {
        byte[] keyBytes = new byte[key.remaining()];
        key.get(keyBytes);
        return super.get(getDefaultCFHandle(), totalOrderReadOptions, keyBytes);
    }

    public void write(WriteBatch writeBatch) throws RocksDBException {
        db.write(ableWalWriteOptions, writeBatch);
        accountWriteOpsForWalFlush();
    }

    private void accountWriteOpsForWalFlush() throws RocksDBException {
        int writeCount = writeOpsCounter.incrementAndGet();
        if (writeCount >= messageStoreConfig.getRocksdbFlushWalFrequency()) {
            this.db.flushWal(false);
            writeOpsCounter.getAndAdd(-writeCount);
            this.scheduledExecutorService.submit(() -> {
                try {
                    long start = System.currentTimeMillis();
                    this.db.syncWal();
                    LOGGER.info("WAL fsync of RocksDB[{}] completed, taking {}ms", this.dbPath, System.currentTimeMillis() - start);
                } catch (RocksDBException e) {
                    LOGGER.error("[BUG]Unexpected exception raised while SyncWAL, RocksDB={}", this.dbPath, e);
                }
            });
        }
    }

    public RocksIterator iterate(ByteBuffer beginKey, ByteBuffer endKey) {
        try (ReadOptions readOptions = new ReadOptions()) {
            readOptions.setTotalOrderSeek(true);
            readOptions.setTailing(false);
            readOptions.setAutoPrefixMode(true);
            // Use DirectSlice till the follow issue is fixed:
            // https://github.com/facebook/rocksdb/issues/13098
            //
            // readOptions.setIterateUpperBound(new DirectSlice(endKey));
            byte[] buf = new byte[endKey.remaining()];
            endKey.slice().get(buf);
            readOptions.setIterateUpperBound(new Slice(buf));

            RocksIterator iterator = db.newIterator(defaultCFHandle, readOptions);
            iterator.seek(beginKey.slice());
            return iterator;
        }
    }
}
