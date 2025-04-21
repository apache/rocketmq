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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.UtilAll;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

public class ConfigRocksDBStorage extends AbstractRocksDBStorage {
    public static final byte[] KV_DATA_VERSION_COLUMN_FAMILY_NAME = "kvDataVersion".getBytes(StandardCharsets.UTF_8);
    public static final byte[] FORBIDDEN_COLUMN_FAMILY_NAME = "forbidden".getBytes(StandardCharsets.UTF_8);

    protected ColumnFamilyHandle kvDataVersionFamilyHandle;
    protected ColumnFamilyHandle forbiddenFamilyHandle;
    public static final byte[] KV_DATA_VERSION_KEY = "kvDataVersionKey".getBytes(StandardCharsets.UTF_8);



    public ConfigRocksDBStorage(final String dbPath) {
        this(dbPath, false);
    }

    public ConfigRocksDBStorage(final String dbPath, CompressionType compressionType) {
        this(dbPath, false);
        this.compressionType = compressionType;
    }

    public ConfigRocksDBStorage(final String dbPath, boolean readOnly) {
        super(dbPath);
        this.readOnly = readOnly;
    }

    protected void initOptions() {
        this.options = ConfigHelper.createConfigDBOptions();
        super.initOptions();
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);

            initOptions();

            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

            ColumnFamilyOptions defaultOptions = ConfigHelper.createConfigColumnFamilyOptions();
            this.cfOptions.add(defaultOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(KV_DATA_VERSION_COLUMN_FAMILY_NAME, defaultOptions));
            cfDescriptors.add(new ColumnFamilyDescriptor(FORBIDDEN_COLUMN_FAMILY_NAME, defaultOptions));
            open(cfDescriptors);

            this.defaultCFHandle = cfHandles.get(0);
            this.kvDataVersionFamilyHandle = cfHandles.get(1);
            this.forbiddenFamilyHandle = cfHandles.get(2);

        } catch (final Exception e) {
            AbstractRocksDBStorage.LOGGER.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {
        this.kvDataVersionFamilyHandle.close();
        this.forbiddenFamilyHandle.close();
    }

    public void put(final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws Exception {
        put(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes, keyLen, valueBytes, valueBytes.length);
    }

    public void put(final ByteBuffer keyBB, final ByteBuffer valueBB) throws Exception {
        put(this.defaultCFHandle, this.ableWalWriteOptions, keyBB, valueBB);
    }

    public byte[] get(final byte[] keyBytes) throws Exception {
        return get(this.defaultCFHandle, this.totalOrderReadOptions, keyBytes);
    }

    public void updateKvDataVersion(final byte[] valueBytes) throws Exception {
        put(this.kvDataVersionFamilyHandle, this.ableWalWriteOptions, KV_DATA_VERSION_KEY, KV_DATA_VERSION_KEY.length, valueBytes, valueBytes.length);
    }

    public byte[] getKvDataVersion() throws Exception {
        return get(this.kvDataVersionFamilyHandle, this.totalOrderReadOptions, KV_DATA_VERSION_KEY);
    }

    public void updateForbidden(final byte[] keyBytes, final byte[] valueBytes) throws Exception {
        put(this.forbiddenFamilyHandle, this.ableWalWriteOptions, keyBytes, keyBytes.length, valueBytes, valueBytes.length);
    }

    public byte[] getForbidden(final byte[] keyBytes) throws Exception {
        return get(this.forbiddenFamilyHandle, this.totalOrderReadOptions, keyBytes);
    }

    public void delete(final byte[] keyBytes) throws Exception {
        delete(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes);
    }

    public List<byte[]> multiGet(final List<ColumnFamilyHandle> cfhList, final List<byte[]> keys) throws
        RocksDBException {
        return multiGet(this.totalOrderReadOptions, cfhList, keys);
    }

    public void batchPut(final WriteBatch batch) throws RocksDBException {
        batchPut(this.writeOptions, batch);
    }

    public void batchPutWithWal(final WriteBatch batch) throws RocksDBException {
        batchPut(this.ableWalWriteOptions, batch);
    }

    public RocksIterator iterator() {
        return this.db.newIterator(this.defaultCFHandle, this.totalOrderReadOptions);
    }

    public RocksIterator forbiddenIterator() {
        return this.db.newIterator(this.forbiddenFamilyHandle, this.totalOrderReadOptions);
    }

    public RocksIterator iterator(ReadOptions readOptions) {
        return this.db.newIterator(this.defaultCFHandle, readOptions);
    }
}
