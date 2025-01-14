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
package org.apache.rocketmq.store.timer.rocksdb;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.rocksdb.RocksDBOptionsFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.RocksIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TimerMessageRocksDBStorage extends AbstractRocksDBStorage implements TimerMessageKVStore {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public final static byte[] TRANSACTION_COLUMN_FAMILY = "transaction".getBytes(StandardCharsets.UTF_8);
    public final static byte[] POP_COLUMN_FAMILY = "pop".getBytes(StandardCharsets.UTF_8);
    private final static byte[] TIMER_OFFSET_KEY = "timer_offset".getBytes(StandardCharsets.UTF_8);

    // key : 100 * n (delay time); value : long (msg number)
    private final static byte[] METRIC_COLUMN_FAMILY = "metric".getBytes(StandardCharsets.UTF_8);

    private ColumnFamilyHandle popColumnFamilyHandle;
    private ColumnFamilyHandle transactionColumnFamilyHandle;
    private ColumnFamilyHandle metricColumnFamilyHandle;

    private WriteOptions writeOptions;
    private WriteOptions deleteOptions;

    public TimerMessageRocksDBStorage(String filePath) {
        super(filePath);
    }

    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();

        this.deleteOptions = new WriteOptions();
        this.deleteOptions.setSync(false);
        this.deleteOptions.setDisableWAL(true);
        this.deleteOptions.setNoSlowdown(false);

        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);
        this.writeOptions.setDisableWAL(true);
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
            ColumnFamilyOptions defaultOptions = RocksDBOptionsFactory.createTimerCFOptions();
            ColumnFamilyOptions popOptions = RocksDBOptionsFactory.createTimerCFOptions();
            ColumnFamilyOptions transactionOptions = RocksDBOptionsFactory.createTimerCFOptions();
            ColumnFamilyOptions metricOptions = RocksDBOptionsFactory.createTimerMetricCFOptions();

            this.cfOptions.add(defaultOptions);
            this.cfOptions.add(popOptions);
            this.cfOptions.add(transactionOptions);
            this.cfOptions.add(metricOptions);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(POP_COLUMN_FAMILY, popOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(TRANSACTION_COLUMN_FAMILY, transactionOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(METRIC_COLUMN_FAMILY, metricOptions));

            this.open(cfDescriptors);
            this.defaultCFHandle = cfHandles.get(0);
            this.popColumnFamilyHandle = cfHandles.get(1);
            this.transactionColumnFamilyHandle = cfHandles.get(2);
            this.metricColumnFamilyHandle = cfHandles.get(3);

            log.debug("Timer message on rocksdb init, filePath={}", this.dbPath);
        } catch (final Exception e) {
            log.error("Timer message on rocksdb init error, filePath={}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {
        if (this.writeOptions != null) {
            this.writeOptions.close();
        }
        if (this.deleteOptions != null) {
            this.deleteOptions.close();
        }
        if (this.defaultCFHandle != null) {
            this.defaultCFHandle.close();
        }
        if (this.transactionColumnFamilyHandle != null) {
            this.transactionColumnFamilyHandle.close();
        }
        if (this.popColumnFamilyHandle != null) {
            this.popColumnFamilyHandle.close();
        }
        if (this.metricColumnFamilyHandle != null) {
            this.metricColumnFamilyHandle.close();
        }
    }

    @Override
    public String getFilePath() {
        return this.dbPath;
    }

    @Override
    public void writeDefaultRecords(List<TimerMessageRecord> consumerRecordList, long offset, int timestamp) {
        if (!consumerRecordList.isEmpty()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (TimerMessageRecord record : consumerRecordList) {
                    writeBatch.put(defaultCFHandle, record.getKeyBytes(), record.getValueBytes());
                }
                // atomically update offset
                writeBatch.put(TIMER_OFFSET_KEY, ByteBuffer.allocate(8).putLong(offset).array());
                syncMetric(timestamp / 100, consumerRecordList.size(), writeBatch);
                this.db.write(writeOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException("Write record error", e);
            }
        }
    }

    @Override
    public void writeAssignRecords(byte[] columnFamily, List<TimerMessageRecord> consumerRecordList, long offset, int timestamp) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);

        if (cfHandle != null && !consumerRecordList.isEmpty()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (TimerMessageRecord record : consumerRecordList) {
                    writeBatch.put(cfHandle, record.getKeyBytes(), record.getValueBytes());
                }
                // atomically update offset
                writeBatch.put(TIMER_OFFSET_KEY, ByteBuffer.allocate(8).putLong(offset).array());
                syncMetric(timestamp / 100, consumerRecordList.size(), writeBatch);
                this.db.write(writeOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException("Write record error", e);
            }
        }
    }

    @Override
    public void deleteDefaultRecords(List<TimerMessageRecord> consumerRecordList, int timestamp) {
        if (!consumerRecordList.isEmpty()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (TimerMessageRecord record : consumerRecordList) {
                    writeBatch.delete(defaultCFHandle, record.getKeyBytes());
                }
                syncMetric(timestamp / 100, - consumerRecordList.size(), writeBatch);
                this.db.write(deleteOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException("Delete record error", e);
            }
        }
    }

    @Override
    public void deleteAssignRecords(byte[] columnFamily, List<TimerMessageRecord> consumerRecordList, int timestamp) {
        ColumnFamilyHandle deleteCfHandle = getColumnFamily(columnFamily);;

        if (deleteCfHandle != null && !consumerRecordList.isEmpty()) {
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (TimerMessageRecord record : consumerRecordList) {
                    writeBatch.delete(deleteCfHandle, record.getKeyBytes());
                }
                syncMetric(timestamp / 100, - consumerRecordList.size(), writeBatch);
                this.db.write(deleteOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException("Delete record error", e);
            }
        }
    }

    private ColumnFamilyHandle getColumnFamily(byte[] columnFamily) {
        ColumnFamilyHandle cfHandle;
        if (columnFamily == POP_COLUMN_FAMILY) {
            cfHandle = popColumnFamilyHandle;
        } else if (columnFamily == TRANSACTION_COLUMN_FAMILY) {
            cfHandle = transactionColumnFamilyHandle;
        } else if (columnFamily == RocksDB.DEFAULT_COLUMN_FAMILY) {
            cfHandle = defaultCFHandle;
        } else {
            throw new RuntimeException("Unknown column family");
        }

        return cfHandle;
    }

    @Override
    public List<TimerMessageRecord> scanRecords(byte[] columnFamily, long lowerTime, long upperTime) {
        List<TimerMessageRecord> records = new ArrayList<>();
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);

        try (ReadOptions readOptions = new ReadOptions()
            .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array()))
            .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(upperTime).array()));
            RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
            iterator.seek(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array());
            while (iterator.isValid()) {
                records.add(TimerMessageRecord.decode(iterator.value()));
                iterator.next();
            }
        }

        return records;
    }

    @Override
    public List<TimerMessageRecord> scanExpiredRecords(byte[] columnFamily, long lowerTime, long upperTime, int maxCount) {
        List<TimerMessageRecord> records = new ArrayList<>();
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);

        try (ReadOptions readOptions = new ReadOptions()
                .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array()))
                .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(upperTime).array()));
             RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
            iterator.seek(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array());
            while (iterator.isValid() && records.size() < maxCount) {
                records.add(TimerMessageRecord.decode(iterator.value()));
                iterator.next();
            }
        }

        return records;
    }

    @Override
    public long getCommitOffset() {
        try {
            byte[] offsetBytes = db.get(TIMER_OFFSET_KEY);
            return offsetBytes == null ? 0 : ByteBuffer.wrap(offsetBytes).getLong();
        } catch (RocksDBException e) {
            throw new RuntimeException("Get commit offset error", e);
        }
    }

    @Override
    public int getMetricSize(int lowerTime, int upperTime) {
        int metricSize = 0;

        try (ReadOptions readOptions = new ReadOptions()
                .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array()))
                .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(upperTime).array()));
             RocksIterator iterator = db.newIterator(metricColumnFamilyHandle, readOptions)) {
            iterator.seek(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array());
            while (iterator.isValid()) {
                metricSize += ByteBuffer.wrap(iterator.value()).getInt();
                iterator.next();
            }
        }
        return metricSize;
    }

    private void syncMetric(int key, int update, WriteBatch writeBatch) {
        try {
            writeBatch.put(metricColumnFamilyHandle, ByteBuffer.allocate(4).putInt(key).array(), ByteBuffer.allocate(4).putInt(update).array());
        } catch (RocksDBException e) {
            throw new RuntimeException("Sync metric error", e);
        }
    }
}
