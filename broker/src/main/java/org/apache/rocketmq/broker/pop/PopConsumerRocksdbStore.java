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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private volatile PopConsumerWriteBatchService writeBatchService;
    protected ColumnFamilyHandle columnFamilyHandle;
    private final long blockCacheSize;
    private final long writeBufferSize;
    private final int completionThreads;

    public PopConsumerRocksdbStore(String filePath, long blockCacheSize, long writeBufferSize) {
        this(filePath, blockCacheSize, writeBufferSize, 4);
    }

    public PopConsumerRocksdbStore(String filePath, long blockCacheSize, long writeBufferSize, int completionThreads) {
        super(filePath);
        if (completionThreads <= 0) {
            throw new IllegalArgumentException("POP KV completion thread count must be positive");
        }
        this.completionThreads = completionThreads;
        this.blockCacheSize = blockCacheSize;
        this.writeBufferSize = writeBufferSize;
    }

    // https://www.cnblogs.com/renjc/p/rocksdb-class-db.html
    // https://github.com/johnzeng/rocksdb-doc-cn/blob/master/doc/RocksDB-Tuning-Guide.md
    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();

        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(true);
        this.writeOptions.setDisableWAL(false);
        this.writeOptions.setNoSlowdown(false);

        this.compactRangeOptions = new CompactRangeOptions();
        this.compactRangeOptions.setBottommostLevelCompaction(
            CompactRangeOptions.BottommostLevelCompaction.kForce);
        this.compactRangeOptions.setAllowWriteStall(true);
        this.compactRangeOptions.setExclusiveManualCompaction(false);
        this.compactRangeOptions.setChangeLevel(true);
        this.compactRangeOptions.setTargetLevel(-1);
        this.compactRangeOptions.setMaxSubcompactions(4);
    }

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

    @Override
    public synchronized boolean start() {
        if (!super.start()) {
            return false;
        }
        if (writeBatchService == null) {
            writeBatchService = new PopConsumerWriteBatchService(this::writeBatch, completionThreads);
            writeBatchService.start();
        }
        return true;
    }

    @Override
    public synchronized boolean shutdown() {
        if (writeBatchService != null) {
            // Stop admission and drain before closing any native handles.
            writeBatchService.shutdown();
        }
        boolean shutdown = super.shutdown();
        if (shutdown) {
            writeBatchService = null;
        }
        return shutdown;
    }

    @Override
    public void writeRecords(List<PopConsumerRecord> records) {
        updateRecords(records, Collections.emptyList(), false).join();
    }

    @Override
    public void deleteRecords(List<PopConsumerRecord> records) {
        updateRecords(Collections.emptyList(), records, false).join();
    }

    @Override
    public CompletableFuture<Void> updateRecordsAsync(List<PopConsumerRecord> puts, List<PopConsumerRecord> deletes) {
        return updateRecords(puts, deletes, true);
    }

    private CompletableFuture<Void> updateRecords(List<PopConsumerRecord> puts, List<PopConsumerRecord> deletes,
        boolean asyncCompletion) {
        if (puts.isEmpty() && deletes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        // Snapshot mutable records on submission, not when the writer eventually runs.
        List<PopConsumerWriteBatchService.Mutation> mutations = new ArrayList<>(puts.size() + deletes.size());
        try {
            for (PopConsumerRecord record : puts) {
                mutations.add(new PopConsumerWriteBatchService.Mutation(record.getKeyBytes(), record.getValueBytes()));
            }
            for (PopConsumerRecord record : deletes) {
                mutations.add(new PopConsumerWriteBatchService.Mutation(record.getKeyBytes(), null));
            }
            PopConsumerWriteBatchService service = writeBatchService;
            if (service != null) {
                return service.submit(mutations, asyncCompletion);
            }
            throw new IllegalStateException("POP KV store has not been started");
        } catch (Exception e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    CompletableFuture<Void> submitCacheRequest(PopConsumerWriteBatchService.Request request) {
        PopConsumerWriteBatchService service = writeBatchService;
        if (service == null) {
            request.future.completeExceptionally(new IllegalStateException("POP KV store has not been started"));
            return request.future;
        }
        return service.submit(request);
    }

    protected void writeBatch(List<PopConsumerWriteBatchService.Mutation> mutations) throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            for (PopConsumerWriteBatchService.Mutation mutation : mutations) {
                if (mutation.value == null) {
                    batch.delete(columnFamilyHandle, mutation.key);
                } else {
                    batch.put(columnFamilyHandle, mutation.key, mutation.value);
                }
            }
            db.write(writeOptions, batch);
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
        if (this.columnFamilyHandle != null) {
            this.columnFamilyHandle.close();
        }
    }
}
