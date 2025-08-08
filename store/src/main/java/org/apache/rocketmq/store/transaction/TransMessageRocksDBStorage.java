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
package org.apache.rocketmq.store.transaction;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.rocksdb.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

public class TransMessageRocksDBStorage extends AbstractRocksDBStorage {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final byte[] LAST_OFFSET_PY = "lastOffsetPy".getBytes(StandardCharsets.UTF_8);
    private static final int LAST_OFFSET_PY_LENGTH = LAST_OFFSET_PY.length;

    public TransMessageRocksDBStorage(String filePath) {
        super(filePath);
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();
            ColumnFamilyOptions defaultOptions = RocksDBOptionsFactory.createTransCFOptions();
            this.cfOptions.add(defaultOptions);
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            this.open(cfDescriptors);
            defaultCFHandle = cfHandles.get(0);
            log.info("TransMessageRocksDBStorage init success, dbPath: {}", this.dbPath);
        } catch (final Exception e) {
            logError.error("TransMessageRocksDBStorage init error, dbPath: {}, error: {}", this.dbPath, e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();
        super.initOptions();
    }

    @Override
    protected void preShutdown() {
        log.info("TransMessageRocksDBStorage pre shutdown success, dbPath: {}", this.dbPath);
    }

    public String getFilePath() {
        return this.dbPath;
    }

    public void writeRecords(byte[] columnFamily, List<TransRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        long lastOffsetPy = 0L;
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (TransRocksDBRecord record : recordList) {
                if (null == record) {
                    logError.error("TransMessageRocksDBStorage writeRecords error, record is null");
                    continue;
                }
                byte[] keyBytes = record.getKeyBytes();
                if (null == keyBytes || keyBytes.length == 0) {
                    logError.error("TransMessageRocksDBStorage writeRecords param error, keyBytes: {}", keyBytes);
                    continue;
                }
                if (record.isOp()) {
                    writeBatch.delete(cfHandle, record.getKeyBytes());
                } else {
                    byte[] valueBytes = record.getValueBytes();
                    if (null == valueBytes || valueBytes.length == 0) {
                        logError.error("TransMessageRocksDBStorage writeRecords param error, valueBytes: {}", valueBytes);
                        continue;
                    }
                    writeBatch.put(cfHandle, keyBytes, valueBytes);
                    lastOffsetPy = Math.max(lastOffsetPy, record.getOffsetPy());
                }
            }
            if (lastOffsetPy > 0L) {
                Long lastOffsetPyStore = getLastOffsetPy(columnFamily);
                if (null == lastOffsetPyStore || lastOffsetPy > lastOffsetPyStore) {
                    writeBatch.put(LAST_OFFSET_PY, ByteBuffer.allocate(Long.BYTES).putLong(lastOffsetPy).array());
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStorage writeRecords error: {}", e.getMessage());
        }
    }

    public void updateRecords(byte[] columnFamily, List<TransRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (TransRocksDBRecord record : recordList) {
                if (null == record) {
                    logError.error("TransMessageRocksDBStorage updateRecords error, record is null");
                    continue;
                }
                byte[] keyBytes = record.getKeyBytes();
                byte[] valueBytes = record.getValueBytes();
                if (null == keyBytes || keyBytes.length == 0 || null == valueBytes || valueBytes.length == 0) {
                    logError.error("TransMessageRocksDBStorage updateRecords param error, keyBytes: {}, valueBytes: {}", keyBytes, valueBytes);
                    continue;
                }
                if (record.isDelete()) {
                    writeBatch.delete(cfHandle, keyBytes);
                } else {
                    writeBatch.put(cfHandle, keyBytes, valueBytes);
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStorage updateRecords error: {}", e.getMessage());
        }
    }

    public List<TransRocksDBRecord> scanRecords(byte[] columnFamily, int size, byte[] startKey) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || size <= 0) {
            return null;
        }
        RocksIterator iterator = null;
        try {
            iterator = db.newIterator(cfHandle);
            if (null == startKey || startKey.length == 0) {
                iterator.seekToFirst();
            } else {
                iterator.seek(startKey);
                iterator.next();
            }
            List<TransRocksDBRecord> records = new ArrayList<>();
            for (; iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (null == key || key.length == 0 || key.length == LAST_OFFSET_PY_LENGTH && Arrays.equals(key, LAST_OFFSET_PY)) {
                    continue;
                }
                TransRocksDBRecord transRocksDBRecord = null;
                try {
                    transRocksDBRecord = TransRocksDBRecord.decode(key, iterator.value());
                } catch (Exception e) {
                    logError.error("TransMessageRocksDBStorage scanRecords error: {}", e.getMessage());
                }
                if (null != transRocksDBRecord) {
                    records.add(transRocksDBRecord);
                }
                if (records.size() >= size) {
                    break;
                }
            }
            return records;
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStorage scanRecords error: {}", e.getMessage());
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
        return null;
    }

    public Long getLastOffsetPy(byte[] columnFamily) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle) {
            return null;
        }
        try {
            byte[] offsetBytes = get(cfHandle, readOptions, LAST_OFFSET_PY);
            return offsetBytes == null ? 0L : ByteBuffer.wrap(offsetBytes).getLong();
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStorage getLastOffsetPy error: {}", e.getMessage());
            return null;
        }
    }

    public TransRocksDBRecord getRecord(byte[] columnFamily, TransRocksDBRecord transRocksDBRecord) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || null == transRocksDBRecord) {
            return null;
        }
        try {
            byte[] keyBytes = transRocksDBRecord.getKeyBytes();
            if (null == keyBytes) {
                return null;
            }
            byte[] valueBytes = get(cfHandle, readOptions, keyBytes);
            if (null == valueBytes || valueBytes.length != TransRocksDBRecord.VALUE_LENGTH) {
                return null;
            }
            return TransRocksDBRecord.decode(keyBytes, valueBytes);
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStorage getRecord error: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public synchronized boolean shutdown() {
        try {
            boolean result = super.shutdown();
            log.info("shutdown TransMessageRocksDBStorage result: {}", result);
            return result;
        } catch (Exception e) {
            log.error("shutdown TransMessageRocksDBStorage error: {}", e.getMessage());
            return false;
        }
    }

    private ColumnFamilyHandle getColumnFamily(byte[] columnFamily) {
        if (columnFamily == RocksDB.DEFAULT_COLUMN_FAMILY) {
            return defaultCFHandle;
        }
        throw new RuntimeException("Unknown column family");
    }

}
