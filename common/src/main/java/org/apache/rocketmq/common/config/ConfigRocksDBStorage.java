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
package org.apache.rocketmq.common.config;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

public class ConfigRocksDBStorage extends AbstractRocksDBStorage {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public static final ConcurrentMap<String, ConfigRocksDBStorage> STORE_MAP = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ColumnFamilyHandle> columnFamilyNameHandleMap;
    private ColumnFamilyOptions columnFamilyOptions;

    private ConfigRocksDBStorage(final String dbPath, boolean readOnly, CompressionType compressionType) {
        super(dbPath);
        this.readOnly = readOnly;
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        this.columnFamilyNameHandleMap = new ConcurrentHashMap<>();
    }

    public ConfigRocksDBStorage(final String dbPath, boolean readOnly) {
        this(dbPath, readOnly, null);
    }

    protected void initOptions() {
        this.options = ConfigHelper.createConfigDBOptions();
        this.columnFamilyOptions = ConfigHelper.createConfigColumnFamilyOptions();
        this.cfOptions.add(columnFamilyOptions);
        super.initOptions();
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);

            initOptions();

            List<byte[]> columnFamilyNames = new ArrayList<>(RocksDB.listColumnFamilies(
                new Options(options, columnFamilyOptions), dbPath));
            addIfNotExists(columnFamilyNames, RocksDB.DEFAULT_COLUMN_FAMILY);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            for (byte[] columnFamilyName : columnFamilyNames) {
                cfDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName, columnFamilyOptions));
            }

            this.open(cfDescriptors);
            for (int i = 0; i < columnFamilyNames.size(); i++) {
                columnFamilyNameHandleMap.put(new String(columnFamilyNames.get(i), CHARSET), cfHandles.get(i));
            }
            this.defaultCFHandle = columnFamilyNameHandleMap.get(new String(RocksDB.DEFAULT_COLUMN_FAMILY, CHARSET));
        } catch (final Exception e) {
            AbstractRocksDBStorage.LOGGER.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {
        for (final ColumnFamilyHandle columnFamilyHandle : this.columnFamilyNameHandleMap.values()) {
            if (columnFamilyHandle.isOwningHandle()) {
                columnFamilyHandle.close();
            }
        }
    }

    // batch operations
    public void writeBatchPutOperation(String cf, WriteBatch writeBatch, final byte[] key, final byte[] value) throws RocksDBException {
        writeBatch.put(getOrCreateColumnFamily(cf), key, value);
    }

    public void batchPut(final WriteBatch batch) throws RocksDBException {
        batchPut(this.writeOptions, batch);
    }

    public void batchPutWithWal(final WriteBatch batch) throws RocksDBException {
        batchPut(this.ableWalWriteOptions, batch);
    }


    // operations with the specified cf
    public void put(String cf, final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws Exception {
        put(getOrCreateColumnFamily(cf), this.ableWalWriteOptions, keyBytes, keyLen, valueBytes, valueBytes.length);
    }

    public void put(String cf, final ByteBuffer keyBB, final ByteBuffer valueBB) throws Exception {
        put(getOrCreateColumnFamily(cf), this.ableWalWriteOptions, keyBB, valueBB);
    }

    public byte[] get(String cf, final byte[] keyBytes) throws Exception {
        ColumnFamilyHandle columnFamilyHandle = columnFamilyNameHandleMap.get(cf);
        if (columnFamilyHandle == null) {
            return null;
        }
        return get(columnFamilyHandle, this.totalOrderReadOptions, keyBytes);
    }

    public void delete(String cf, final byte[] keyBytes) throws Exception {
        ColumnFamilyHandle columnFamilyHandle = columnFamilyNameHandleMap.get(cf);
        if (columnFamilyHandle == null) {
            return;
        }
        delete(columnFamilyHandle, this.ableWalWriteOptions, keyBytes);
    }

    public void iterate(final String cf, BiConsumer<byte[], byte[]> biConsumer) throws RocksDBException {
        if (!hold()) {
            LOGGER.warn("RocksDBKvStore[path={}] has been shut down", dbPath);
            return;
        }
        ColumnFamilyHandle columnFamilyHandle = columnFamilyNameHandleMap.get(cf);
        if (columnFamilyHandle == null) {
            return;
        }
        try (RocksIterator iterator = this.db.newIterator(columnFamilyHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                biConsumer.accept(iterator.key(), iterator.value());
            }
            iterator.status();
        }
    }

    public RocksIterator iterator() {
        return this.db.newIterator(this.defaultCFHandle, this.totalOrderReadOptions);
    }

    public ColumnFamilyHandle getOrCreateColumnFamily(String cf) throws RocksDBException {
        if (!columnFamilyNameHandleMap.containsKey(cf)) {
            if (readOnly) {
                String errInfo = String.format("RocksDBKvStore[path=%s] is open as read-only", dbPath);
                LOGGER.warn(errInfo);
                throw new RocksDBException(errInfo);
            }
            synchronized (this) {
                if (!columnFamilyNameHandleMap.containsKey(cf)) {
                    ColumnFamilyDescriptor columnFamilyDescriptor =
                        new ColumnFamilyDescriptor(cf.getBytes(CHARSET), columnFamilyOptions);
                    ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
                    columnFamilyNameHandleMap.putIfAbsent(cf, columnFamilyHandle);
                    cfHandles.add(columnFamilyHandle);
                }
            }
        }
        return columnFamilyNameHandleMap.get(cf);
    }

    public void addIfNotExists(List<byte[]> columnFamilyNames, byte[] byteArray) {
        if (columnFamilyNames.stream().noneMatch(array -> Arrays.equals(array, byteArray))) {
            columnFamilyNames.add(byteArray);
        }
    }

    public static ConfigRocksDBStorage getStore(String path, boolean readOnly, CompressionType compressionType) {
        return ConcurrentHashMapUtils.computeIfAbsent(STORE_MAP, path,
            k -> new ConfigRocksDBStorage(path, readOnly, compressionType));
    }

    public static ConfigRocksDBStorage getStore(String path, boolean readOnly) {
        return getStore(path, readOnly, null);
    }

    public static void shutdown(String path) {
        ConfigRocksDBStorage kvStore = STORE_MAP.remove(path);
        if (kvStore != null) {
            kvStore.shutdown();
        }
    }

    public static void destroy(String path) {
        ConfigRocksDBStorage kvStore = STORE_MAP.remove(path);
        if (kvStore != null) {
            kvStore.shutdown();
            kvStore.destroy();
        }
    }
}
