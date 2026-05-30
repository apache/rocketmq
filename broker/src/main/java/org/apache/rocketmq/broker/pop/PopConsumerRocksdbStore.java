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
package org.apache.rocketmq.broker.pop;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.rocksdb.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopConsumerRocksdbStore extends AbstractRocksDBStorage implements PopConsumerKVStore {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final byte[] COLUMN_FAMILY_NAME = "popState".getBytes(StandardCharsets.UTF_8);

    private WriteOptions writeOptions;
    private WriteOptions deleteOptions;
    protected ColumnFamilyHandle columnFamilyHandle;
    private final long blockCacheSize;
    private final long writeBufferSize;

    public PopConsumerRocksdbStore(String filePath, long blockCacheSize, long writeBufferSize) {
        super(filePath);
        this.blockCacheSize = blockCacheSize;
        this.writeBufferSize = writeBufferSize;
    }

    /**
     * Configure RocksDB options for Pop consumer record storage.
     *
     * <p>Unlike the parent class defaults, write and delete options enable
     * WAL and synchronous flush — Pop visibility state is the sole source
     * of truth and must survive crashes. Compaction is configured to be
     * aggressive so that expired-then-deleted records are purged promptly,
     * reclaiming disk space.
     *
     * @see <a href="https://www.cnblogs.com/renjc/p/rocksdb-class-db.html">rocksdb-class-db</a>
     * @see <a href="https://github.com/johnzeng/rocksdb-doc-cn/blob/master/doc/RocksDB-Tuning-Guide.md">RocksDB-Tuning-Guide</a>
     */
    protected void initOptions() {
        // durability-first: enable WAL and sync flush for pop state recovery
        this.options = RocksDBOptionsFactory.createDBOptions();

        this.writeOptions = new WriteOptions();
        // fsync every write to disk
        this.writeOptions.setSync(true);
        // enable WAL
        this.writeOptions.setDisableWAL(false);
        // allow writing throttling under pressure
        this.writeOptions.setNoSlowdown(false);

        // delete must be durable too — otherwise ack can be lost and message revived incorrectly
        this.deleteOptions = new WriteOptions();
        this.deleteOptions.setSync(true);
        this.deleteOptions.setDisableWAL(false);
        this.deleteOptions.setNoSlowdown(false);

        // aggressive compaction to purge expired pop records and reclaim space
        this.compactRangeOptions = new CompactRangeOptions();
        // force compact bottom level
        this.compactRangeOptions.setBottommostLevelCompaction(
            CompactRangeOptions.BottommostLevelCompaction.kForce);
        // allow compaction to pause writes
        this.compactRangeOptions.setAllowWriteStall(true);
        // manual compaction runs in parallel with auto-compaction.
        // Appropriate here because expired Pop records generate tombstones continuously,
        // and cleanup should not starve RocksDB's normal background work
        this.compactRangeOptions.setExclusiveManualCompaction(false);
        // Allows compaction to move data across levels
        this.compactRangeOptions.setChangeLevel(true);
        // -1 delegates level selection to RocksDB's internal heuristics
        this.compactRangeOptions.setTargetLevel(-1);
        // Splits the compaction work into at most 4 parallel sub-tasks
        this.compactRangeOptions.setMaxSubcompactions(4);
    }

    /**
     * Initialise the RocksDB instance with a dedicated column family for Pop state.
     *
     * <p>Two column families are created:
     * <ol>
     *   <li>{@code default} — unused, required by RocksDB</li>
     *   <li>{@code "popState"} — stores Pop consumer records keyed by
     *       {@code visibilityTimeout|groupId@topicId@queueId@offset}</li>
     * </ol>
     *
     * <p>Called by {@link AbstractRocksDBStorage#start()} before the storage
     * is marked as loaded. Returns {@code false} if any step fails, preventing
     * all subsequent read/write operations via {@link #hold()}.
     *
     * @return {@code true} if the database was opened successfully
     */
    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();

            // init column family here
            ColumnFamilyOptions defaultOptions = RocksDBOptionsFactory.createPopCFOptions(blockCacheSize, writeBufferSize);
            ColumnFamilyOptions popStateOptions = RocksDBOptionsFactory.createPopCFOptions(blockCacheSize, writeBufferSize);
            this.cfOptions.add(defaultOptions);
            this.cfOptions.add(popStateOptions);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(COLUMN_FAMILY_NAME, popStateOptions));
            this.open(cfDescriptors);
            this.defaultCFHandle = cfHandles.get(0);
            this.columnFamilyHandle = cfHandles.get(1);

            log.debug("PopConsumerRocksdbStore init, filePath={}", this.dbPath);
        } catch (final Exception e) {
            log.error("PopConsumerRocksdbStore init error, filePath={}", this.dbPath, e);
            return false;
        }
        return true;
    }

    public String getFilePath() {
        return this.dbPath;
    }

    /**
     * Batch-write consumer records to RocksDB via a single {@link WriteBatch}.
     *
     * <p>Each record is serialized with its visibility-timeout-prefixed key
     * so that {@link #scanExpiredRecords} can efficiently scan by time range.
     *
     * @param consumerRecordList the records to persist
     */
    @Override
    public void writeRecords(List<PopConsumerRecord> consumerRecordList) {
        if (!consumerRecordList.isEmpty()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (PopConsumerRecord record : consumerRecordList) {
                    writeBatch.put(columnFamilyHandle, record.getKeyBytes(), record.getValueBytes());
                }
                this.db.write(writeOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException("Write record error", e);
            }
        }
    }

    @Override
    public void deleteRecords(List<PopConsumerRecord> consumerRecordList) {
        if (!consumerRecordList.isEmpty()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (PopConsumerRecord record : consumerRecordList) {
                    writeBatch.delete(columnFamilyHandle, record.getKeyBytes());
                }
                this.db.write(deleteOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException("Delete record error", e);
            }
        }
    }

    @Override
    // https://github.com/facebook/rocksdb/issues/10300
    public List<PopConsumerRecord> scanExpiredRecords(long lower, long upper, int maxCount) {
        // In RocksDB, we can use SstPartitionerFixedPrefixFactory in cfOptions
        // and new ColumnFamilyOptions().useFixedLengthPrefixExtractor() to
        // configure prefix indexing to improve the performance of scans.
        // However, in the current implementation, this is not the bottleneck.
        List<PopConsumerRecord> consumerRecordList = new ArrayList<>();
        try (ReadOptions scanOptions = new ReadOptions()
            .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(lower).array()))
            .setIterateUpperBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(upper).array()));
             RocksIterator iterator = db.newIterator(this.columnFamilyHandle, scanOptions)) {
            iterator.seek(ByteBuffer.allocate(Long.BYTES).putLong(lower).array());
            while (iterator.isValid() && consumerRecordList.size() < maxCount) {
                consumerRecordList.add(PopConsumerRecord.decode(iterator.value()));
                iterator.next();
            }
        }
        return consumerRecordList;
    }

    @Override
    protected void preShutdown() {
        if (this.writeOptions != null) {
            this.writeOptions.close();
        }
        if (this.deleteOptions != null) {
            this.deleteOptions.close();
        }
        if (this.columnFamilyHandle != null) {
            this.columnFamilyHandle.close();
        }
    }
}
