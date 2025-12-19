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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.index.rocksdb.IndexRocksDBRecord;
import org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord;
import org.apache.rocketmq.store.transaction.TransRocksDBRecord;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import static org.apache.rocketmq.common.MixAll.dealTimeToHourStamps;
import static org.apache.rocketmq.common.MixAll.getHours;
import static org.apache.rocketmq.common.MixAll.isHourTime;
import static org.apache.rocketmq.store.index.rocksdb.IndexRocksDBRecord.KEY_SPLIT;
import static org.apache.rocketmq.store.index.rocksdb.IndexRocksDBRecord.KEY_SPLIT_BYTES;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_DELETE;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_PUT;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_UPDATE;

public class MessageRocksDBStorage extends AbstractRocksDBStorage {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final String ROCKSDB_MESSAGE_DIRECTORY = "rocksdbstore";

    public static final byte[] TIMER_COLUMN_FAMILY = "timer".getBytes(StandardCharsets.UTF_8);
    public static final byte[] TRANS_COLUMN_FAMILY = "trans".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LAST_OFFSET_PY = "lastOffsetPy".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LAST_STORE_TIMESTAMP = "lastStoreTimeStamp".getBytes(StandardCharsets.UTF_8);
    private static final byte[] END_SUFFIX_BYTES = new byte[512];
    static {
        Arrays.fill(END_SUFFIX_BYTES, (byte) 0xFF);
    }
    private static final Set<byte[]> COMMON_CHECK_POINT_KEY_SET_FOR_TIMER = new HashSet<>();
    public static final byte[] SYS_TOPIC_SCAN_OFFSET_CHECK_POINT = "sys_topic_scan_offset_checkpoint".getBytes(StandardCharsets.UTF_8);
    public static final byte[] TIMELINE_CHECK_POINT = "timeline_checkpoint".getBytes(StandardCharsets.UTF_8);
    static {
        COMMON_CHECK_POINT_KEY_SET_FOR_TIMER.add(SYS_TOPIC_SCAN_OFFSET_CHECK_POINT);
        COMMON_CHECK_POINT_KEY_SET_FOR_TIMER.add(TIMELINE_CHECK_POINT);
    }
    private static final byte[] DELETE_VAL_FLAG = new byte[] {(byte)0xFF};
    private static final int LAST_OFFSET_PY_LENGTH = LAST_OFFSET_PY.length;

    private volatile ColumnFamilyHandle timerCFHandle;
    private volatile ColumnFamilyHandle transCFHandle;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final Cache<byte[], byte[]> DELETE_KEY_CACHE_FOR_TIMER = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .build();

    public MessageRocksDBStorage(MessageStoreConfig messageStoreConfig) {
        super(Paths.get(messageStoreConfig.getStorePathRootDir(), ROCKSDB_MESSAGE_DIRECTORY).toString());
        this.start();
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();
            ColumnFamilyOptions indexCFOptions = RocksDBOptionsFactory.createIndexCFOptions();
            ColumnFamilyOptions timerCFOptions = RocksDBOptionsFactory.createTimerCFOptions();
            ColumnFamilyOptions transCFOptions = RocksDBOptionsFactory.createTransCFOptions();
            this.cfOptions.add(indexCFOptions);
            this.cfOptions.add(timerCFOptions);
            this.cfOptions.add(transCFOptions);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, indexCFOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(TIMER_COLUMN_FAMILY, timerCFOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(TRANS_COLUMN_FAMILY, transCFOptions));
            this.open(cfDescriptors);
            this.defaultCFHandle = cfHandles.get(0);
            this.timerCFHandle = cfHandles.get(1);
            this.transCFHandle = cfHandles.get(2);
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    db.flush(flushOptions, timerCFHandle);
                    log.info("MessageRocksDBStorage flush timer wal success");
                } catch (Exception e) {
                    logError.error("MessageRocksDBStorage flush timer wal failed, error: {}", e.getMessage());
                }
            }, 5, 5, TimeUnit.MINUTES);

            log.info("MessageRocksDBStorage init success, dbPath: {}", this.dbPath);
        } catch (final Exception e) {
            logError.error("MessageRocksDBStorage init error, dbPath: {}, error: {}", this.dbPath, e.getMessage());
            return false;
        }
        return true;
    }

    protected void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();
        super.initOptions();
    }

    public String getFilePath() {
        return this.dbPath;
    }

    @Override
    protected void preShutdown() {
        log.info("MessageRocksDBStorage pre shutdown success, dbPath: {}", this.dbPath);
    }

    public List<Long> queryOffsetForIndex(byte[] columnFamily, String topic, String indexType, String key, long beginTime, long endTime, int maxNum, String lastKey) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || StringUtils.isEmpty(topic) || StringUtils.isEmpty(indexType) || StringUtils.isEmpty(key) || beginTime < 0L || endTime <= 0L || beginTime > endTime || maxNum <= 0) {
            logError.error("MessageRocksDBStorage queryOffsetForIndex param error, cfHandle: {}, topic: {}, indexType: {}, key: {}, beginTime: {}, endTime: {}, maxNum: {}", cfHandle, topic, indexType, key, beginTime, endTime, maxNum);
            return null;
        }
        Long lastIndexTime = getLastIndexTimeForIndex(lastKey);
        if (!StringUtils.isEmpty(lastKey) && (null == lastIndexTime || lastIndexTime <= 0L || !isHourTime(lastIndexTime))) {
            logError.error("MessageRocksDBStorage queryOffsetForIndex parse and check lastIndexTime error, lastIndexTime: {}, lastKey: {}", lastIndexTime, lastKey);
            return null;
        }
        List<Long> hours = getHours(beginTime, endTime);
        if (CollectionUtils.isEmpty(hours)) {
            logError.error("MessageRocksDBStorage queryOffsetForIndex param error, hours is empty, beginTime: {}, endTime: {}", beginTime, endTime);
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
                logError.error("MessageRocksDBStorage queryOffsetForIndex error, seekKeyBytes is null");
                return null;
            }
            try (RocksIterator iterator = db.newIterator(cfHandle, readOptions)) {
                for (iterator.seek(seekKeyBytes); iterator.isValid(); iterator.next()) {
                    try {
                        byte[] currentKeyBytes = iterator.key();
                        if (null == currentKeyBytes || currentKeyBytes.length == 0) {
                            break;
                        }
                        if (null != lastKeyBytes && currentKeyBytes.length == lastKeyBytes.length && MixAll.isByteArrayEqual(currentKeyBytes, 0, currentKeyBytes.length, lastKeyBytes, 0, lastKeyBytes.length)) {
                            continue;
                        }
                        if (currentKeyBytes.length < keyPrefixBytes.length || !MixAll.isByteArrayEqual(currentKeyBytes, 0, keyPrefixBytes.length, keyPrefixBytes, 0, keyPrefixBytes.length)) {
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
                        logError.error("MessageRocksDBStorage queryOffsetForIndex iterator error: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                logError.error("MessageRocksDBStorage queryOffsetForIndex error: {}", e.getMessage());
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
            log.error("MessageRocksDBStorage lastKeyToBytes split error, lastKey: {}", lastKey);
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
            log.error("MessageRocksDBStorage lastKeyToBytes error, lastKey: {}, error: {}", lastKey, e.getMessage());
            return null;
        }
    }

    public void deleteRecordsForIndex(byte[] columnFamily, long storeTime, int hours) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || storeTime < 0L || hours <= 0) {
            logError.error("MessageRocksDBStorage deleteRecordsForIndex param error, storeTime: {}, hours: {}", storeTime, hours);
            return;
        }
        long endTime = dealTimeToHourStamps(storeTime);
        long startTime = endTime - TimeUnit.HOURS.toMillis(hours);
        try {
            byte[] startKey = ByteBuffer.allocate(Long.BYTES + KEY_SPLIT_BYTES.length).putLong(startTime).put(KEY_SPLIT_BYTES).array();
            byte[] endKey = ByteBuffer.allocate(Long.BYTES + KEY_SPLIT_BYTES.length + END_SUFFIX_BYTES.length).putLong(endTime).put(KEY_SPLIT_BYTES).put(END_SUFFIX_BYTES).array();
            rangeDelete(cfHandle, ableWalWriteOptions, startKey, endKey);
            log.info("MessageRocksDBStorage deleteRecordsForIndex delete success, storeTime: {}, hours: {}", storeTime, hours);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage deleteRecordsForIndex delete error, storeTime: {}, hours: {}, error: {}", storeTime, hours, e.getMessage());
        }
    }

    public void writeRecordsForIndex(byte[] columnFamily, List<IndexRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (IndexRocksDBRecord record : recordList) {
                try {
                    if (null == record) {
                        logError.warn("MessageRocksDBStorage writeRecordsForIndex error, record is null");
                        continue;
                    }
                    byte[] keyBytes = record.getKeyBytes();
                    byte[] valueBytes = record.getValueBytes();
                    if (null == keyBytes || keyBytes.length == 0 || null == valueBytes || valueBytes.length == 0) {
                        logError.error("MessageRocksDBStorage writeRecordsForIndex param error, keyBytes: {}, valueBytes: {}", keyBytes, valueBytes);
                        continue;
                    }
                    writeBatch.put(cfHandle, keyBytes, valueBytes);
                } catch (Exception e) {
                    logError.error("MessageRocksDBStorage writeRecordsForIndex error: {}", e.getMessage());
                }
            }
            IndexRocksDBRecord lastRecord = recordList.get(recordList.size() - 1);
            if (null != lastRecord && StringUtils.isEmpty(lastRecord.getKey()) && StringUtils.isEmpty(lastRecord.getTag())) {
                long offset = lastRecord.getOffsetPy();
                Long lastOffsetPy = getLastOffsetPy(columnFamily);
                if (null == lastOffsetPy || offset > lastOffsetPy) {
                    writeBatch.put(cfHandle, LAST_OFFSET_PY, ByteBuffer.allocate(Long.BYTES).putLong(offset).array());
                }
                long storeTime = lastRecord.getStoreTime();
                Long lastStoreTimeStamp = getLastStoreTimeStampForIndex(columnFamily);
                if (null == lastStoreTimeStamp || storeTime > lastStoreTimeStamp) {
                    writeBatch.put(cfHandle, LAST_STORE_TIMESTAMP, ByteBuffer.allocate(Long.BYTES).putLong(storeTime).array());
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage writeRecordsForIndex error: {}", e.getMessage());
        }
    }

    public Long getLastStoreTimeStampForIndex(byte[] columnFamily) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle) {
            return null;
        }
        try {
            byte[] storeTime = get(cfHandle, readOptions, LAST_STORE_TIMESTAMP);
            return null == storeTime ? 0L : ByteBuffer.wrap(storeTime).getLong();
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage getLastStoreTimeStampForIndex error: {}", e.getMessage());
            return null;
        }
    }

    private static Long getLastIndexTimeForIndex(String lastKey) {
        if (StringUtils.isEmpty(lastKey)) {
            return null;
        }
        try {
            String[] split = lastKey.split(KEY_SPLIT);
            if (split.length > 0) {
                return Long.valueOf(split[0]);
            }
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage getLastIndexTimeForIndex error lastKey: {}, e: {}", lastKey, e.getMessage());
        }
        return null;
    }

    public void writeRecordsForTimer(byte[] columnFamily, List<TimerRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (TimerRocksDBRecord record : recordList) {
                if (null == record) {
                    logError.error("MessageRocksDBStorage writeRecordsForTimer error, record is null");
                    continue;
                }
                try {
                    byte[] keyBytes = record.getKeyBytes();
                    byte[] valueBytes = record.getValueBytes();
                    if (null == keyBytes || keyBytes.length == 0 || null == valueBytes || valueBytes.length == 0) {
                        logError.error("MessageRocksDBStorage writeRecordsForTimer param error, keyBytes: {}, valueBytes: {}", keyBytes, valueBytes);
                        continue;
                    }
                    if (record.getActionFlag() == TIMER_ROCKSDB_PUT) {
                        writeBatch.put(cfHandle, keyBytes, valueBytes);
                    } else if (record.getActionFlag() == TIMER_ROCKSDB_DELETE) {
                        writeBatch.delete(cfHandle, keyBytes);
                        DELETE_KEY_CACHE_FOR_TIMER.put(keyBytes, DELETE_VAL_FLAG);
                    } else if (record.getActionFlag() == TIMER_ROCKSDB_UPDATE) {
                        byte[] deleteByte = DELETE_KEY_CACHE_FOR_TIMER.getIfPresent(keyBytes);
                        if (null == deleteByte) {
                            writeBatch.put(cfHandle, keyBytes, valueBytes);
                        }
                    } else {
                        logError.error("MessageRocksDBStorage writeRecordsForTimer record actionFlag error, actionFlag: {}", record.getActionFlag());
                    }
                } catch (Exception e) {
                    logError.error("MessageRocksDBStorage writeRecordsForTimer error: {}", e.getMessage());
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage writeRecordsForTimer error: {}", e.getMessage());
        }
    }

    public List<TimerRocksDBRecord> scanRecordsForTimer(byte[] columnFamily, long lowerTime, long upperTime, int size, byte[] startKey) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || lowerTime <= 0L || upperTime <= 0L || lowerTime > upperTime || size <= 0) {
            return null;
        }
        RocksIterator iterator = null;
        try (ReadOptions readOptions = new ReadOptions()
            .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array()))
            .setIterateUpperBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(upperTime).array()))) {
            iterator = db.newIterator(cfHandle, readOptions);
            if (null == startKey || startKey.length == 0) {
                iterator.seek(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array());
            } else {
                iterator.seek(startKey);
                iterator.next();
            }
            List<TimerRocksDBRecord> records = new ArrayList<>();
            for (; iterator.isValid(); iterator.next()) {
                try {
                    TimerRocksDBRecord timerRocksDBRecord = TimerRocksDBRecord.decode(iterator.key(), iterator.value());
                    if (null == timerRocksDBRecord) {
                        logError.error("MessageRocksDBStorage scanRecordsForTimer error, decode timerRocksDBRecord is null");
                        continue;
                    }
                    records.add(timerRocksDBRecord);
                    if (records.size() >= size) {
                        break;
                    }
                } catch (Exception e) {
                    logError.error("MessageRocksDBStorage scanRecordsForTimer iterator error: {}", e.getMessage());
                }
            }
            return records;
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage scanRecordsForTimer error: {}", e.getMessage());
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
        return null;
    }

    public void deleteRecordsForTimer(byte[] columnFamily, long lowerTime, long upperTime) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || lowerTime <= 0L || upperTime <= 0L || lowerTime > upperTime) {
            logError.error("MessageRocksDBStorage deleteRecordsForTimer param error, cfHandle: {}, lowerTime: {}, upperTime: {}", cfHandle, lowerTime, upperTime);
            return;
        }
        byte[] startKey = ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array();
        byte[] endKey = ByteBuffer.allocate(Long.BYTES + END_SUFFIX_BYTES.length).putLong(upperTime).put(END_SUFFIX_BYTES).array();
        try {
            rangeDelete(cfHandle, ableWalWriteOptions, startKey, endKey);
            log.info("MessageRocksDBStorage deleteRecordsForTimer success, lowerTime: {}, upperTime: {}", lowerTime, upperTime);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage deleteRecordsForTimer param error, lowerTime: {}, upperTime: {}, error: {}", lowerTime, upperTime, e.getMessage());
        }
    }

    public void writeCheckPointForTimer(byte[] columnFamily, byte[] key, long value) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || !COMMON_CHECK_POINT_KEY_SET_FOR_TIMER.contains(key) || value < 0L) {
            logError.error("MessageRocksDBStorage writeCheckPointForTimer param error, cfHandle: {}, key: {}, value: {}", cfHandle, key, value);
            return;
        }
        try {
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
            put(cfHandle, ableWalWriteOptions, key, key.length, valueBytes, valueBytes.length);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage writeCheckPointForTimer error: {}", e.getMessage());
        }
    }

    public long getCheckpointForTimer(byte[] columnFamily, byte[] key) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || !COMMON_CHECK_POINT_KEY_SET_FOR_TIMER.contains(key)) {
            logError.error("MessageRocksDBStorage getCheckpointForTimer error, cfHandle: {}, key: {}", cfHandle, key);
            return 0L;
        }
        try {
            byte[] checkpoint = get(cfHandle, readOptions, key);
            if (null == checkpoint && Arrays.equals(key, TIMELINE_CHECK_POINT)) {
                return (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10)) / TimeUnit.SECONDS.toMillis(1) * TimeUnit.SECONDS.toMillis(1);
            }
            return checkpoint == null ? 0L : ByteBuffer.wrap(checkpoint).getLong();
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage getCheckpointForTimer error: {}", e.getMessage());
            return 0L;
        }
    }

    public void deleteCheckPointForTimer(byte[] columnFamily, byte[] key) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || !COMMON_CHECK_POINT_KEY_SET_FOR_TIMER.contains(key)) {
            logError.error("MessageRocksDBStorage deleteCheckPointForTimer error, cfHandle: {}, key: {}", cfHandle, key);
            return;
        }
        try {
            delete(cfHandle, ableWalWriteOptions, key);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage deleteCheckPointForTimer error: {}", e.getMessage());
            throw new RuntimeException("MessageRocksDBStorage deleteCheckPointForTimer error", e);
        }
    }

    public void writeRecordsForTrans(byte[] columnFamily, List<TransRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        long lastOffsetPy = 0L;
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (TransRocksDBRecord record : recordList) {
                if (null == record) {
                    logError.error("MessageRocksDBStorage writeRecordsForTrans error, record is null");
                    continue;
                }
                byte[] keyBytes = record.getKeyBytes();
                if (null == keyBytes || keyBytes.length == 0) {
                    logError.error("MessageRocksDBStorage writeRecordsForTrans param error, keyBytes: {}", keyBytes);
                    continue;
                }
                if (record.isOp()) {
                    writeBatch.delete(cfHandle, record.getKeyBytes());
                } else {
                    byte[] valueBytes = record.getValueBytes();
                    if (null == valueBytes || valueBytes.length == 0) {
                        logError.error("MessageRocksDBStorage writeRecordsForTrans param error, valueBytes: {}", valueBytes);
                        continue;
                    }
                    writeBatch.put(cfHandle, keyBytes, valueBytes);
                    lastOffsetPy = Math.max(lastOffsetPy, record.getOffsetPy());
                }
            }
            if (lastOffsetPy > 0L) {
                Long lastOffsetPyStore = getLastOffsetPy(columnFamily);
                if (null == lastOffsetPyStore || lastOffsetPy > lastOffsetPyStore) {
                    writeBatch.put(cfHandle, LAST_OFFSET_PY, ByteBuffer.allocate(Long.BYTES).putLong(lastOffsetPy).array());
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("MessageRocksDBStorage writeRecordsForTrans error: {}", e.getMessage());
        }
    }

    public void updateRecordsForTrans(byte[] columnFamily, List<TransRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (TransRocksDBRecord record : recordList) {
                if (null == record) {
                    logError.error("MessageRocksDBStorage updateRecordsForTrans error, record is null");
                    continue;
                }
                byte[] keyBytes = record.getKeyBytes();
                byte[] valueBytes = record.getValueBytes();
                if (null == keyBytes || keyBytes.length == 0 || null == valueBytes || valueBytes.length == 0) {
                    logError.error("MessageRocksDBStorage updateRecordsForTrans param error, keyBytes: {}, valueBytes: {}", keyBytes, valueBytes);
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
            logError.error("MessageRocksDBStorage updateRecordsForTrans error: {}", e.getMessage());
        }
    }

    public List<TransRocksDBRecord> scanRecordsForTrans(byte[] columnFamily, int size, byte[] startKey) {
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
                    logError.error("MessageRocksDBStorage scanRecordsForTrans error: {}", e.getMessage());
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
            logError.error("MessageRocksDBStorage scanRecordsForTrans error: {}", e.getMessage());
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
        return null;
    }

    public TransRocksDBRecord getRecordForTrans(byte[] columnFamily, TransRocksDBRecord transRocksDBRecord) {
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
            logError.error("MessageRocksDBStorage getRecordForTrans error: {}", e.getMessage());
            return null;
        }
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
            logError.error("MessageRocksDBStorage getLastOffsetPy error: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public synchronized boolean shutdown() {
        try {
            boolean result = super.shutdown();
            log.info("shutdown MessageRocksDBStorage result: {}", result);
            return result;
        } catch (Exception e) {
            logError.error("shutdown MessageRocksDBStorage error : {}", e.getMessage());
            return false;
        }
    }

    private ColumnFamilyHandle getColumnFamily(byte[] columnFamily) {
        if (Arrays.equals(columnFamily, RocksDB.DEFAULT_COLUMN_FAMILY)) {
            return this.defaultCFHandle;
        } else if (Arrays.equals(columnFamily, TIMER_COLUMN_FAMILY)) {
            return this.timerCFHandle;
        } else if (Arrays.equals(columnFamily, TRANS_COLUMN_FAMILY)) {
            return this.transCFHandle;
        }
        throw new RuntimeException("Unknown column family");
    }
}
