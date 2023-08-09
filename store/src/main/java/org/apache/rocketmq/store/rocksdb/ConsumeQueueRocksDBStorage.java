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

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.store.MessageStore;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class ConsumeQueueRocksDBStorage extends AbstractRocksDBStorage {
    private final MessageStore messageStore;
    private volatile ColumnFamilyHandle offsetCFHandle;

    public ConsumeQueueRocksDBStorage(final MessageStore messageStore, final String dbPath, final int prefixLen) {
        this.messageStore = messageStore;
        this.dbPath = dbPath;
        this.readOnly = false;
    }

    private void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();

        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);
        this.writeOptions.setDisableWAL(true);
        this.writeOptions.setNoSlowdown(true);

        this.totalOrderReadOptions = new ReadOptions();
        this.totalOrderReadOptions.setPrefixSameAsStart(false);
        this.totalOrderReadOptions.setTotalOrderSeek(false);

        this.compactRangeOptions = new CompactRangeOptions();
        this.compactRangeOptions.setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce);
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

            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList();

            ColumnFamilyOptions cqCfOptions = RocksDBOptionsFactory.createCQCFOptions(this.messageStore);
            this.cfOptions.add(cqCfOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cqCfOptions));

            ColumnFamilyOptions offsetCfOptions = RocksDBOptionsFactory.createOffsetCFOptions();
            this.cfOptions.add(offsetCfOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor("offset".getBytes(DataConverter.CHARSET_UTF8), offsetCfOptions));

            final List<ColumnFamilyHandle> cfHandles = new ArrayList();
            open(cfDescriptors, cfHandles);

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
}