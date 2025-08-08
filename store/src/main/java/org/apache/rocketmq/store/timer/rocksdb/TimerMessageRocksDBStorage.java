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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.rocksdb.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_DELETE;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_PUT;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_UPDATE;

public class TimerMessageRocksDBStorage extends AbstractRocksDBStorage {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final Set<byte[]> COMMON_CHECK_POINT_KEY_SET = new HashSet<>();
    public static final byte[] SYS_TOPIC_SCAN_OFFSET_CHECK_POINT = "sys_topic_scan_offset_checkpoint".getBytes(StandardCharsets.UTF_8);
    public static final byte[] TIMELINE_CHECK_POINT = "timeline_checkpoint".getBytes(StandardCharsets.UTF_8);
    static {
        COMMON_CHECK_POINT_KEY_SET.add(SYS_TOPIC_SCAN_OFFSET_CHECK_POINT);
        COMMON_CHECK_POINT_KEY_SET.add(TIMELINE_CHECK_POINT);
    }
    private static final byte[] END_SUFFIX_BYTES = new byte[64];
    static {
        Arrays.fill(END_SUFFIX_BYTES, (byte) 0xFF);
    }
    private static final Cache<byte[], byte[]> DELETE_KEY_CACHE = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .build();
    private static final byte[] DELETE_VAL_FLAG = new byte[] {(byte)0xFF};
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public TimerMessageRocksDBStorage(String filePath) {
        super(filePath);
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();
            ColumnFamilyOptions defaultOptions = RocksDBOptionsFactory.createTimerCFOptions();
            this.cfOptions.add(defaultOptions);
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            this.open(cfDescriptors);
            defaultCFHandle = cfHandles.get(0);
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    db.flushWal(true);
                    log.info("TimerMessageRocksDBStorage flush wal success");
                } catch (Exception e) {
                    logError.error("TimerMessageRocksDBStorage flush wal failed, error: {}", e.getMessage());
                }
            }, 5, 5, TimeUnit.MINUTES);
            log.info("TimerMessageRocksDBStorage init success, dbPath: {}", this.dbPath);
        } catch (final Exception e) {
            logError.error("TimerMessageRocksDBStorage init error, dbPath: {}, error: {}", this.dbPath, e.getMessage());
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
        log.info("TimerMessageRocksDBStorage pre shutdown success, dbPath: {}", this.dbPath);
    }

    public String getFilePath() {
        return this.dbPath;
    }

    public void writeRecords(byte[] columnFamily, List<TimerRocksDBRecord> recordList) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || CollectionUtils.isEmpty(recordList)) {
            return;
        }
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (TimerRocksDBRecord record : recordList) {
                if (null == record) {
                    logError.error("TimerMessageRocksDBStorage writeRecords error, record is null");
                    continue;
                }
                try {
                    byte[] keyBytes = record.getKeyBytes();
                    byte[] valueBytes = record.getValueBytes();
                    if (null == keyBytes || keyBytes.length == 0 || null == valueBytes || valueBytes.length == 0) {
                        logError.error("TimerMessageRocksDBStorage writeRecords param error, keyBytes: {}, valueBytes: {}", keyBytes, valueBytes);
                        continue;
                    }
                    if (record.getActionFlag() == TIMER_ROCKSDB_PUT) {
                        writeBatch.put(cfHandle, keyBytes, valueBytes);
                    } else if (record.getActionFlag() == TIMER_ROCKSDB_DELETE) {
                        writeBatch.delete(cfHandle, keyBytes);
                        DELETE_KEY_CACHE.put(keyBytes, DELETE_VAL_FLAG);
                    } else if (record.getActionFlag() == TIMER_ROCKSDB_UPDATE) {
                        byte[] deleteByte = DELETE_KEY_CACHE.getIfPresent(keyBytes);
                        if (null == deleteByte) {
                            writeBatch.put(cfHandle, keyBytes, valueBytes);
                        }
                    } else {
                        logError.error("TimerMessageRocksDBStorage record actionFlag error, actionFlag: {}", record.getActionFlag());
                    }
                } catch (Exception e) {
                    logError.error("TimerMessageRocksDBStorage writeRecords error: {}", e.getMessage());
                }
            }
            batchPut(ableWalWriteOptions, writeBatch);
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStorage writeRecords error: {}", e.getMessage());
        }
    }

    public List<TimerRocksDBRecord> scanRecords(byte[] columnFamily, long lowerTime, long upperTime, int size, byte[] startKey) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || lowerTime <= 0L || upperTime <= 0L || lowerTime > upperTime || size <= 0) {
            return null;
        }
        RocksIterator iterator = null;
        try (ReadOptions readOptions = new ReadOptions()
            .setIterateLowerBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array()))
            .setIterateUpperBound(new Slice(ByteBuffer.allocate(Long.BYTES).putLong(upperTime).array()))
            .setPrefixSameAsStart(true)) {
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
                        logError.error("TimerMessageRocksDBStorage scanRecords error, decode timerRocksDBRecord is null");
                        continue;
                    }
                    records.add(timerRocksDBRecord);
                    if (records.size() >= size) {
                        break;
                    }
                } catch (Exception e) {
                    logError.error("TimerMessageRocksDBStorage scanRecords iterator error: {}", e.getMessage());
                }
            }
            return records;
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStorage scanRecords error: {}", e.getMessage());
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
        return null;
    }

    public void rangeDeleteRecords(byte[] columnFamily, long lowerTime, long upperTime) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || lowerTime <= 0L || upperTime <= 0L || lowerTime > upperTime) {
            logError.error("TimerMessageRocksDBStorage rangeDeleteRecords param error, cfHandle: {}, lowerTime: {}, upperTime: {}", cfHandle, lowerTime, upperTime);
            return;
        }
        byte[] startKey = ByteBuffer.allocate(Long.BYTES).putLong(lowerTime).array();
        byte[] endKey = ByteBuffer.allocate(Long.BYTES + END_SUFFIX_BYTES.length).putLong(upperTime).put(END_SUFFIX_BYTES).array();
        try {
            rangeDelete(cfHandle, ableWalWriteOptions, startKey, endKey);
            log.info("TimerMessageRocksDBStorage rangeDeleteRecords success, lowerTime: {}, upperTime: {}", lowerTime, upperTime);
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStorage rangeDeleteRecords param error, lowerTime: {}, upperTime: {}, error: {}", lowerTime, upperTime, e.getMessage());
        }
    }

    public void writeCheckPoint(byte[] columnFamily, byte[] key, long value) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || !COMMON_CHECK_POINT_KEY_SET.contains(key) || value < 0L) {
            logError.error("TimerMessageRocksDBStorage writeCheckPoint param error, cfHandle: {}, key: {}, value: {}", cfHandle, key, value);
            return;
        }
        try {
            byte[] valueBytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
            put(cfHandle, ableWalWriteOptions, key, key.length, valueBytes, valueBytes.length);
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStorage writeCheckPoint error: {}", e.getMessage());
        }
    }

    public long getCheckpoint(byte[] columnFamily, byte[] key) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || !COMMON_CHECK_POINT_KEY_SET.contains(key)) {
            logError.error("TimerMessageRocksDBStorage getCheckpoint error, cfHandle: {}, key: {}", cfHandle, key);
            return 0L;
        }
        try {
            byte[] checkpoint = get(cfHandle, readOptions, key);
            if (null == checkpoint && Arrays.equals(key, TIMELINE_CHECK_POINT)) {
                return (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10)) / TimeUnit.SECONDS.toMillis(1) * TimeUnit.SECONDS.toMillis(1);
            }
            return checkpoint == null ? 0L : ByteBuffer.wrap(checkpoint).getLong();
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStorage getCheckpoint error: {}", e.getMessage());
            return 0L;
        }
    }

    public void deleteCheckPoint(byte[] columnFamily, byte[] key) {
        ColumnFamilyHandle cfHandle = getColumnFamily(columnFamily);
        if (null == cfHandle || !COMMON_CHECK_POINT_KEY_SET.contains(key)) {
            logError.error("TimerMessageRocksDBStorage deleteCheckPoint error, cfHandle: {}, key: {}", cfHandle, key);
            return;
        }
        try {
            delete(cfHandle, ableWalWriteOptions, key);
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStorage deleteCheckPoint error: {}", e.getMessage());
            throw new RuntimeException("TimerMessageRocksDBStorage deleteCheckPoint error", e);
        }
    }

    private ColumnFamilyHandle getColumnFamily(byte[] columnFamily) {
        if (columnFamily == RocksDB.DEFAULT_COLUMN_FAMILY) {
            return defaultCFHandle;
        }
        throw new RuntimeException("Unknown column family of TimerMessageRocksDBStorage");
    }

    @Override
    public synchronized boolean shutdown() {
        try {
            boolean result = super.shutdown();
            log.info("shutdown TimerMessageRocksDBStorage result: {}", result);
            return result;
        } catch (Exception e) {
            logError.error("shutdown TimerMessageRocksDBStorage error : {}", e.getMessage());
            return false;
        }
    }

}
