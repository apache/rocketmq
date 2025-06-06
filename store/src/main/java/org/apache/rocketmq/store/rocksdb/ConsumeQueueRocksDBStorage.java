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
package org.apache.rocketmq.store.rocksdb;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.store.MessageStore;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;

public class ConsumeQueueRocksDBStorage extends AbstractRocksDBStorage {

    public static final byte[] OFFSET_COLUMN_FAMILY = "offset".getBytes(StandardCharsets.UTF_8);

    private final MessageStore messageStore;
    private volatile ColumnFamilyHandle offsetCFHandle;

    public ConsumeQueueRocksDBStorage(final MessageStore messageStore, final String dbPath) {
        super(dbPath);
        this.messageStore = messageStore;
        this.readOnly = false;
    }

    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();
        super.initOptions();
    }

    @Override
    protected void initTotalOrderReadOptions() {
        this.totalOrderReadOptions = new ReadOptions();
        this.totalOrderReadOptions.setPrefixSameAsStart(false);
        this.totalOrderReadOptions.setTotalOrderSeek(false);
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);

            initOptions();

            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

            ColumnFamilyOptions cqCfOptions = RocksDBOptionsFactory.createCQCFOptions(this.messageStore);
            this.cfOptions.add(cqCfOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cqCfOptions));

            ColumnFamilyOptions offsetCfOptions = RocksDBOptionsFactory.createOffsetCFOptions();
            this.cfOptions.add(offsetCfOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(OFFSET_COLUMN_FAMILY, offsetCfOptions));
            open(cfDescriptors);
            this.defaultCFHandle = cfHandles.get(0);
            this.offsetCFHandle = cfHandles.get(1);
        } catch (final Exception e) {
            LOGGER.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {
        this.offsetCFHandle.close();
    }

    public byte[] getCQ(final byte[] keyBytes) throws RocksDBException {
        return get(this.defaultCFHandle, this.totalOrderReadOptions, keyBytes);
    }

    public byte[] getOffset(final byte[] keyBytes) throws RocksDBException {
        return get(this.offsetCFHandle, this.totalOrderReadOptions, keyBytes);
    }

    public List<byte[]> multiGet(final List<ColumnFamilyHandle> cfhList, final List<byte[]> keys) throws RocksDBException {
        return multiGet(this.totalOrderReadOptions, cfhList, keys);
    }

    public void batchPut(final WriteBatch batch) throws RocksDBException {
        batchPut(this.writeOptions, batch);
    }

    public void manualCompaction(final long minPhyOffset) {
        try {
            manualCompaction(minPhyOffset, this.compactRangeOptions);
        } catch (Exception e) {
            LOGGER.error("manualCompaction Failed. minPhyOffset: {}", minPhyOffset, e);
        }
    }

    public RocksIterator seekOffsetCF() {
        return this.db.newIterator(this.offsetCFHandle, this.totalOrderReadOptions);
    }

    public ColumnFamilyHandle getOffsetCFHandle() {
        return this.offsetCFHandle;
    }


    public void iterate(ColumnFamilyHandle columnFamilyHandle, final byte[] prefix, BiConsumer<byte[], byte[]> callback)
        throws RocksDBException {

        if (ArrayUtils.isEmpty(prefix)) {
            throw new RocksDBException("Prefix is not allowed to be null");
        }

        iterate(columnFamilyHandle, prefix, null, null, callback);
    }

    public void iterate(ColumnFamilyHandle columnFamilyHandle, byte[] prefix,
        final byte[] start, final byte[] end, BiConsumer<byte[], byte[]> callback) throws RocksDBException {

        if (ArrayUtils.isEmpty(prefix) && ArrayUtils.isEmpty(start)) {
            throw new RocksDBException("To determine lower boundary, prefix and start may not be null at the same "
                + "time.");
        }

        if (ArrayUtils.isEmpty(prefix) && ArrayUtils.isEmpty(end)) {
            throw new RocksDBException("To determine upper boundary, prefix and end may not be null at the same time.");
        }

        if (columnFamilyHandle == null) {
            return;
        }

        ReadOptions readOptions = null;
        Slice startSlice = null;
        Slice endSlice = null;
        Slice prefixSlice = null;
        RocksIterator iterator = null;
        try {
            readOptions = new ReadOptions();
            readOptions.setTotalOrderSeek(true);
            readOptions.setReadaheadSize(4L * 1024 * 1024);
            boolean hasStart = !ArrayUtils.isEmpty(start);
            boolean hasPrefix = !ArrayUtils.isEmpty(prefix);

            if (hasStart) {
                startSlice = new Slice(start);
                readOptions.setIterateLowerBound(startSlice);
            }

            if (!ArrayUtils.isEmpty(end)) {
                endSlice = new Slice(end);
                readOptions.setIterateUpperBound(endSlice);
            }

            if (!hasStart && hasPrefix) {
                prefixSlice = new Slice(prefix);
                readOptions.setIterateLowerBound(prefixSlice);
            }

            iterator = db.newIterator(columnFamilyHandle, readOptions);
            if (hasStart) {
                iterator.seek(start);
            } else if (hasPrefix) {
                iterator.seek(prefix);
            }

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (hasPrefix && !checkPrefix(key, prefix)) {
                    break;
                }
                callback.accept(iterator.key(), iterator.value());
                iterator.next();
            }
        } finally {
            if (startSlice != null) {
                startSlice.close();
            }
            if (endSlice != null) {
                endSlice.close();
            }
            if (prefixSlice != null) {
                prefixSlice.close();
            }
            if (readOptions != null) {
                readOptions.close();
            }
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    private boolean checkPrefix(byte[] key, byte[] upperBound) {
        if (key.length < upperBound.length) {
            return false;
        }
        for (int i = 0; i < upperBound.length; i++) {
            if (key[i] > upperBound[i]) {
                return false;
            }
        }
        return true;
    }
}
