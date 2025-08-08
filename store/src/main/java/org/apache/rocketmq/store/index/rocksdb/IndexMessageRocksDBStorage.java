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
package org.apache.rocketmq.store.index.rocksdb;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
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
import static org.apache.rocketmq.common.MixAll.dealTimeToHourStamps;
import static org.apache.rocketmq.common.MixAll.getHours;
import static org.apache.rocketmq.common.MixAll.isHourTime;
import static org.apache.rocketmq.store.index.rocksdb.IndexRocksDBRecord.KEY_SPLIT;
import static org.apache.rocketmq.store.index.rocksdb.IndexRocksDBRecord.KEY_SPLIT_BYTES;

public class IndexMessageRocksDBStorage extends AbstractRocksDBStorage {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final byte[] LAST_OFFSET_PY = "lastOffsetPy".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LAST_STORE_TIMESTAMP = "lastStoreTimeStamp".getBytes(StandardCharsets.UTF_8);
    private static final byte[] END_SUFFIX_BYTES = new byte[512];
    static {
        Arrays.fill(END_SUFFIX_BYTES, (byte) 0xFF);
    }

    public IndexMessageRocksDBStorage(String dbPath) {
        super(dbPath);
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();
            ColumnFamilyOptions defaultOptions = RocksDBOptionsFactory.createIndexCFOptions();
            this.cfOptions.add(defaultOptions);
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            this.open(cfDescriptors);
            this.defaultCFHandle = cfHandles.get(0);
            log.info("IndexMessageRocksDBStorage init success, dbPath: {}", this.dbPath);
        } catch (final Exception e) {
            logError.error("IndexMessageRocksDBStorage init error, dbPath: {}, error: {}", this.dbPath, e.getMessage());
            return false;
        }
        return true;
    }

    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();
        super.initOptions();
    }

    @Override
    protected void preShutdown() {
        log.info("IndexMessageRocksDBStorage pre shutdown success, dbPath: {}", this.dbPath);
    }

    public String getFilePath() {
        return this.dbPath;
    }

    public List<Long> queryOffset(byte[] columnFamily, String topic, String indexType, String key, long beginTime, long endTime, int maxNum, String lastKey) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || StringUtils.isEmpty(topic) || StringUtils.isEmpty(indexType) || StringUtils.isEmpty(key) || beginTime < 0L || endTime <= 0L || beginTime > endTime || maxNum <= 0) {
            logError.error("IndexMessageRocksDBStorage queryOffset param error, cfHandle: {}, topic: {}, indexType: {}, key: {}, beginTime: {}, endTime: {}, maxNum: {}", cfHandle, topic, indexType, key, beginTime, endTime, maxNum);
            return null;
        }
        Long lastIndexTime = getLastIndexTime(lastKey);
        if (!StringUtils.isEmpty(lastKey) && (null == lastIndexTime || lastIndexTime <= 0L || !isHourTime(lastIndexTime))) {
            logError.error("IndexMessageRocksDBStorage queryOffset parse and check lastIndexTime error, lastIndexTime: {}, lastKey: {}", lastIndexTime, lastKey);
            return null;
        }
        List<Long> hours = getHours(beginTime, endTime);
        if (CollectionUtils.isEmpty(hours)) {
            logError.error("IndexMessageRocksDBStorage queryOffset param error, hours is empty, beginTime: {}, endTime: {}", beginTime, endTime);
            return null;
        }
        List<Long> offsetPyList = new ArrayList<>(maxNum);
        String keyMiddleStr = KEY_SPLIT + topic + KEY_SPLIT + indexType + KEY_SPLIT + key + KEY_SPLIT;
        byte[] keyMiddleBytes = keyMiddleStr.getBytes(StandardCharsets.UTF_8);
        for (Long hour : hours) {
            if (null == hour || null != lastIndexTime && hour < lastIndexTime) {
                continue;
            }
            byte[] seekKeyBytes = null;
            byte[] lastKeyBytes = null;
            byte[] keyPrefixBytes = ByteBuffer.allocate(Long.BYTES + keyMiddleBytes.length).putLong(hour).put(keyMiddleBytes).array();
            if (!StringUtils.isEmpty(lastKey) && hour.equals(lastIndexTime)) {
                seekKeyBytes = lastKeyToBytes(lastKey);
                lastKeyBytes = seekKeyBytes;
            } else {
                seekKeyBytes = keyPrefixBytes;
            }
            if (null == seekKeyBytes) {
                logError.error("IndexMessageRocksDBStorage queryOffset error, seekKeyBytes is null");
                return null;
            }
            try (RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
                for (iterator.seek(seekKeyBytes); iterator.isValid(); iterator.next()) {
                    try {
                        byte[] currentKeyBytes = iterator.key();
                        if (null == currentKeyBytes || currentKeyBytes.length == 0) {
                            break;
                        }
                        if (null != lastKeyBytes && currentKeyBytes.length == lastKeyBytes.length && MixAll.isByteArrayEqual(currentKeyBytes,0, currentKeyBytes.length, lastKeyBytes,0, lastKeyBytes.length)) {
                            continue;
                        }
                        if (currentKeyBytes.length < keyPrefixBytes.length || !MixAll.isByteArrayEqual(currentKeyBytes,0, keyPrefixBytes.length, keyPrefixBytes, 0, keyPrefixBytes.length)) {
                            break;
                        }
                        ByteBuffer valueBuffer = ByteBuffer.wrap(iterator.value());
                        long storeTime = valueBuffer.getLong();
                        if (storeTime >= beginTime && storeTime <= endTime) {
                            byte[] indexKey = iterator.key();
                            if (null == indexKey || indexKey.length < Long.BYTES) {
                                continue;
                            }
                            byte[] bytes = Arrays.copyOfRange(indexKey, indexKey.length - Long.BYTES, indexKey.length);
                            long offset = ByteBuffer.wrap(bytes).getLong();
                            offsetPyList.add(offset);
                            if (offsetPyList.size() >= maxNum) {
                                return offsetPyList;
                            }
                        }
                    } catch (Exception e) {
                        logError.error("IndexMessageRocksDBStorage queryOffset iterator error: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                logError.error("IndexMessageRocksDBStorage queryOffset error: {}", e.getMessage());
            }
        }
        return offsetPyList;
    }

    private byte[] lastKeyToBytes(String lastKey) {
        if (StringUtils.isEmpty(lastKey)) {
            return null;
        }
        String[] split = lastKey.split(KEY_SPLIT);
        if (split.length != 6) {
            log.error("IndexMessageRocksDBStorage lastKeyToBytes split error, lastKey: {}", lastKey);
            return null;
        }
        try {
            long storeTimeHour = Long.parseLong(split[0]);
            long offsetPy = Long.parseLong(split[split.length - 1]);
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 1; i < split.length - 1; i++) {
                stringBuilder.append(KEY_SPLIT).append(split[i]);
            }
            byte[] middleKeyBytes = stringBuilder.append(KEY_SPLIT).toString().getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(Long.BYTES + middleKeyBytes.length + Long.BYTES).putLong(storeTimeHour).put(middleKeyBytes).putLong(offsetPy).array();
        } catch (Exception e) {
            log.error("IndexMessageRocksDBStorage lastKeyToBytes error, lastKey: {}, error: {}", lastKey, e.getMessage());
            return null;
        }
    }

    public void deleteRecords(byte[] columnFamily, long storeTime, int hours) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || storeTime < 0L || hours <= 0) {
            logError.error("IndexMessageRocksDBStorage deleteRecords param error, storeTime: {}, hours: {}", storeTime, hours);
            return;
        }
        long endTime = dealTimeToHourStamps(storeTime);
        long startTime = endTime - TimeUnit.HOURS.toMillis(hours);
        try {
            byte[] startKey = ByteBuffer.allocate(Long.BYTES + KEY_SPLIT_BYTES.length).putLong(startTime).put(KEY_SPLIT_BYTES).array();
            byte[] endKey = ByteBuffer.allocate(Long.BYTES + KEY_SPLIT_BYTES.length + END_SUFFIX_BYTES.length).putLong(endTime).put(KEY_SPLIT_BYTES).put(END_SUFFIX_BYTES).array();
            rangeDelete(cfHandle, ableWalWriteOptions, startKey, endKey);
            log.info("IndexMessageRocksDBStorage deleteRecords delete success, storeTime: {}, hours: {}", storeTime, hours);
        } catch (Exception e) {
            logError.error("IndexMessageRocksDBStorage deleteRecords delete error, storeTime: {}, hours: {}, error: {}", storeTime, hours, e.getMessage());
        }
    }

    public void writeRecords(byte[] columnFamily, List<IndexRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (IndexRocksDBRecord record : recordList) {
                try {
                    if (null == record) {
                        logError.warn("IndexMessageRocksDBStorage writeRecords error, record is null");
                        continue;
                    }
                    byte[] keyBytes = record.getKeyBytes();
                    byte[] valueBytes = record.getValueBytes();
                    if (null == keyBytes || keyBytes.length == 0 || null == valueBytes || valueBytes.length == 0) {
                        logError.error("IndexMessageRocksDBStorage writeRecords param error, keyBytes: {}, valueBytes: {}", keyBytes, valueBytes);
                        continue;
                    }
                    writeBatch.put(cfHandle, keyBytes, valueBytes);
                } catch (Exception e) {
                    logError.error("IndexMessageRocksDBStorage writeRecords error: {}", e.getMessage());
                }
            }
            IndexRocksDBRecord lastRecord = recordList.get(recordList.size() - 1);
            if (null != lastRecord) {
                long offset = lastRecord.getOffsetPy();
                Long lastOffsetPy = getLastOffsetPy(columnFamily);
                if (null == lastOffsetPy || offset > lastOffsetPy) {
                    writeBatch.put(cfHandle, LAST_OFFSET_PY, ByteBuffer.allocate(Long.BYTES).putLong(offset).array());
                }
                long storeTime = lastRecord.getStoreTime();
                Long lastStoreTimeStamp = getLastStoreTimeStamp(columnFamily);
                if (null == lastStoreTimeStamp || storeTime > lastStoreTimeStamp) {
                    writeBatch.put(cfHandle, LAST_STORE_TIMESTAMP, ByteBuffer.allocate(Long.BYTES).putLong(storeTime).array());
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("IndexMessageRocksDBStorage writeRecords error: {}", e.getMessage());
        }
    }

    public Long getLastOffsetPy(byte[] columnFamily) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle) {
            return null;
        }
        try {
            byte[] offsetBytes = get(cfHandle, readOptions, LAST_OFFSET_PY);
            return null == offsetBytes ? 0L : ByteBuffer.wrap(offsetBytes).getLong();
        } catch (Exception e) {
            logError.error("IndexMessageRocksDBStorage getLastOffsetPy error: {}", e.getMessage());
            return null;
        }
    }

    public Long getLastStoreTimeStamp(byte[] columnFamily) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle) {
            return null;
        }
        try {
            byte[] storeTime = get(cfHandle, readOptions, LAST_STORE_TIMESTAMP);
            return null == storeTime ? 0L : ByteBuffer.wrap(storeTime).getLong();
        } catch (Exception e) {
            logError.error("IndexMessageRocksDBStorage getLastStoreTimeStamp error: {}", e.getMessage());
            return null;
        }
    }

    private static Long getLastIndexTime(String lastKey) {
        if (StringUtils.isEmpty(lastKey)) {
            return null;
        }
        try {
            String[] split = lastKey.split(KEY_SPLIT);
            if (split.length > 0) {
                return Long.valueOf(split[0]);
            }
        } catch (Exception e) {
            logError.error("IndexMessageRocksDBStorage getLastIndexTime error lastKey: {}, e: {}", lastKey, e.getMessage());
        }
        return null;
    }

    @Override
    public synchronized boolean shutdown() {
        try {
            boolean result = super.shutdown();
            log.info("shutdown IndexMessageRocksDBStorage result: {}", result);
            return result;
        } catch (Exception e) {
            logError.error("shutdown IndexMessageRocksDBStorage error : {}", e.getMessage());
            return false;
        }
    }

    private ColumnFamilyHandle getColumnFamily(byte[] columnFamily) {
        if (columnFamily == RocksDB.DEFAULT_COLUMN_FAMILY) {
            return this.defaultCFHandle;
        }
        throw new RuntimeException("Unknown column family");
    }

}
