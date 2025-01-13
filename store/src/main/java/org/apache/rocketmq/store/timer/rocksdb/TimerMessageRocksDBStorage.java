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
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimerMessageRocksDBStorage extends AbstractRocksDBStorage implements TimerMessageKVStore {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public final static byte[] TRANSACTION_COLUMN_FAMILY = "transaction".getBytes();
    public final static byte[] POP_COLUMN_FAMILY = "pop".getBytes();

    private ColumnFamilyHandle popColumnFamilyHandle;
    private ColumnFamilyHandle transactionColumnFamilyHandle;

    private WriteOptions writeOptions;
    private WriteOptions deleteOptions;

    public TimerMessageRocksDBStorage(String filePath) {
        super(filePath);
    }

    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();

        this.deleteOptions = new WriteOptions();
        this.deleteOptions.setSync(false);
        this.deleteOptions.setDisableWAL(false);
        this.deleteOptions.setNoSlowdown(false);

        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);
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
            ColumnFamilyOptions defaultOptions = RocksDBOptionsFactory.createTimerCFOptions();
            ColumnFamilyOptions popOptions = RocksDBOptionsFactory.createTimerCFOptions();
            ColumnFamilyOptions transactionOptions = RocksDBOptionsFactory.createTimerCFOptions();

            this.cfOptions.add(defaultOptions);
            this.cfOptions.add(popOptions);
            this.cfOptions.add(transactionOptions);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(POP_COLUMN_FAMILY, popOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(TRANSACTION_COLUMN_FAMILY, transactionOptions));

            this.open(cfDescriptors);
            this.defaultCFHandle = cfHandles.get(0);
            this.popColumnFamilyHandle = cfHandles.get(1);
            this.transactionColumnFamilyHandle = cfHandles.get(2);

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
    }

    @Override
    public String getFilePath() {
        return this.dbPath;
    }

    @Override
    public void writeDefaultRecords(List<TimerMessageRecord> consumerRecordList) {

    }

    @Override
    public void writeAssignRecords(byte[] columnFamily, List<TimerMessageRecord> consumerRecordList) {

    }

    @Override
    public void deleteDefaultRecords(List<TimerMessageRecord> consumerRecordList) {

    }

    @Override
    public void deleteAssignRecords(byte[] columnFamily, List<TimerMessageRecord> consumerRecordList) {

    }

    @Override
    public List<TimerMessageRecord> scanRecords(byte[] columnFamily, long lowerTime, long upperTime) {
        return Collections.emptyList();
    }

    @Override
    public List<TimerMessageRecord> scanExpiredRecords(byte[] columnFamily, long lowerTime, long upperTime, int maxCount) {
        return Collections.emptyList();
    }
}
