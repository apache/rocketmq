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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;
import org.rocksdb.CompactionOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

public class ConfigRocksDBStorage extends AbstractRocksDBStorage {

    public ConfigRocksDBStorage(final String dbPath) {
        this(dbPath, false);
    }

    public ConfigRocksDBStorage(final String dbPath, boolean readOnly) {
        super();
        this.dbPath = dbPath;
        this.readOnly = readOnly;
    }

    private void initOptions() {
        this.options = createConfigDBOptions();
        this.writeOptions = createWriteOptions(false, true);
        this.ableWalWriteOptions = createWriteOptions(false, false);
        this.readOptions = createReadOptions(true, false);
        this.totalOrderReadOptions = createReadOptions(false, false);
        this.compactRangeOptions = createCompactRangeOptions();
        this.compactionOptions = createCompactionOptions();
    }

    @Override
    protected boolean postLoad() {
        try {
            UtilAll.ensureDirOK(this.dbPath);
            initOptions();

            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            ColumnFamilyOptions defaultOptions = createConfigOptions();
            this.cfOptions.add(defaultOptions);
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));

            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            open(cfDescriptors, cfHandles);

            this.defaultCFHandle = cfHandles.get(0);
        } catch (final Exception e) {
            AbstractRocksDBStorage.LOGGER.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
        return true;
    }

    @Override
    protected void preShutdown() {}

    private ColumnFamilyOptions createConfigOptions() {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
                .setFormatVersion(5)
                .setIndexType(IndexType.kBinarySearch)
                .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch)
                .setBlockSize(32 * SizeUnit.KB)
                .setFilterPolicy(new BloomFilter(16, false))
                .setCacheIndexAndFilterBlocks(false)
                .setCacheIndexAndFilterBlocksWithHighPriority(true)
                .setPinL0FilterAndIndexBlocksInCache(false)
                .setPinTopLevelIndexAndFilter(true)
                .setBlockCache(new LRUCache(4 * SizeUnit.MB, 8, false))
                .setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions()
                .setMaxWriteBufferNumber(2)
                .setWriteBufferSize(8 * SizeUnit.MB)
                .setMinWriteBufferNumberToMerge(1)
                .setTableFormatConfig(blockBasedTableConfig)
                .setMemTableConfig(new SkipListMemTableConfig())
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setNumLevels(7)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setLevel0FileNumCompactionTrigger(4)
                .setLevel0SlowdownWritesTrigger(8)
                .setLevel0StopWritesTrigger(12)
                .setTargetFileSizeBase(64 * SizeUnit.MB)
                .setTargetFileSizeMultiplier(2)
                .setMaxBytesForLevelBase(256 * SizeUnit.MB)
                .setMaxBytesForLevelMultiplier(2)
                .setMergeOperator(new StringAppendOperator())
                .setInplaceUpdateSupport(true);

        return options;
    }

    private DBOptions createConfigDBOptions() {
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return new DBOptions()
                .setDbLogDir(getDBLogDir())
                .setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
                .setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords)
                .setManualWalFlush(true)
                .setMaxTotalWalSize(500 * SizeUnit.MB)
                .setWalSizeLimitMB(0)
                .setWalTtlSeconds(0)
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(-1)
                .setMaxLogFileSize(1 * SizeUnit.GB)
                .setKeepLogFileNum(5)
                .setMaxManifestFileSize(1 * SizeUnit.GB)
                .setAllowConcurrentMemtableWrite(false)
                .setStatistics(statistics)
                .setStatsDumpPeriodSec(600)
                .setAtomicFlush(true)
                .setMaxBackgroundJobs(32)
                .setMaxSubcompactions(4)
                .setParanoidChecks(true)
                .setDelayedWriteRate(16 * SizeUnit.MB)
                .setRateLimiter(new RateLimiter(100 * SizeUnit.MB))
                .setUseDirectIoForFlushAndCompaction(true)
                .setUseDirectReads(true);
    }

    private WriteOptions createWriteOptions(boolean sync, boolean disableWAL) {
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(sync);
        writeOptions.setDisableWAL(disableWAL);
        writeOptions.setNoSlowdown(true);
        return writeOptions;
    }

    private ReadOptions createReadOptions(boolean prefixSameAsStart, boolean totalOrderSeek) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(prefixSameAsStart);
        readOptions.setTotalOrderSeek(totalOrderSeek);
        readOptions.setTailing(false);
        return readOptions;
    }

    private CompactRangeOptions createCompactRangeOptions() {
        CompactRangeOptions compactRangeOptions = new CompactRangeOptions();
        compactRangeOptions.setBottommostLevelCompaction(BottommostLevelCompaction.kForce);
        compactRangeOptions.setAllowWriteStall(true);
        compactRangeOptions.setExclusiveManualCompaction(false);
        compactRangeOptions.setChangeLevel(true);
        compactRangeOptions.setTargetLevel(-1);
        compactRangeOptions.setMaxSubcompactions(4);
        return compactRangeOptions;
    }

    private CompactionOptions createCompactionOptions() {
        CompactionOptions compactionOptions = new CompactionOptions();
        compactionOptions.setCompression(CompressionType.LZ4_COMPRESSION);
        compactionOptions.setMaxSubcompactions(4);
        compactionOptions.setOutputFileSizeLimit(4 * 1024 * 1024 * 1024L);
        return compactionOptions;
    }

    public static String getDBLogDir() {
        String rootPath = System.getProperty("user.home");
        if (StringUtils.isEmpty(rootPath)) {
            return "";
        }
        rootPath = rootPath + File.separator + "logs";
        UtilAll.ensureDirOK(rootPath);
        return rootPath + File.separator + "rocketmqlogs" + File.separator;
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

    public void delete(final byte[] keyBytes) throws Exception {
        delete(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes);
    }

    public List<byte[]> multiGet(final List<ColumnFamilyHandle> cfhList, final List<byte[]> keys) throws RocksDBException {
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

    public void rangeDelete(final byte[] startKey, final byte[] endKey) throws RocksDBException {
        rangeDelete(this.defaultCFHandle, this.writeOptions, startKey, endKey);
    }

    public RocksIterator iterator(ReadOptions readOptions) {
        return this.db.newIterator(this.defaultCFHandle, readOptions);
    }
}
