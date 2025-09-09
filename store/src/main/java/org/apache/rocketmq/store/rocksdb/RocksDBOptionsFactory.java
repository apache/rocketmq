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

import org.apache.rocketmq.common.config.ConfigHelper;
import org.apache.rocketmq.store.MessageStore;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompactionStopStyle;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.util.SizeUnit;

public class RocksDBOptionsFactory {

    public static ColumnFamilyOptions createCQCFOptions(final MessageStore messageStore,
        ConsumeQueueCompactionFilterFactory consumeQueueCompactionFilterFactory) {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().
            setFormatVersion(5).
            setIndexType(IndexType.kBinarySearch).
            setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash).
            setDataBlockHashTableUtilRatio(0.75).
            setBlockSize(32 * SizeUnit.KB).
            setMetadataBlockSize(4 * SizeUnit.KB).
            setFilterPolicy(new BloomFilter(16, false)).
            setCacheIndexAndFilterBlocks(false).
            setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(1024 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        CompactionOptionsUniversal compactionOption = new CompactionOptionsUniversal();
        compactionOption.setSizeRatio(100).
                setMaxSizeAmplificationPercent(25).
                setAllowTrivialMove(true).
                setMinMergeWidth(2).
                setMaxMergeWidth(Integer.MAX_VALUE).
                setStopStyle(CompactionStopStyle.CompactionStopStyleTotalSize).
                setCompressionSizePercent(-1);
        String bottomMostCompressionTypeOpt = messageStore.getMessageStoreConfig()
            .getBottomMostCompressionTypeForConsumeQueueStore();
        String compressionTypeOpt = messageStore.getMessageStoreConfig()
            .getRocksdbCompressionType();
        CompressionType bottomMostCompressionType = CompressionType.getCompressionType(bottomMostCompressionTypeOpt);
        CompressionType compressionType = CompressionType.getCompressionType(compressionTypeOpt);

        return columnFamilyOptions.setMaxWriteBufferNumber(4).
                setWriteBufferSize(32 * SizeUnit.MB).  // speed up cq rocksdb open test
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(blockBasedTableConfig).
                setMemTableConfig(new SkipListMemTableConfig()).
                setCompressionType(compressionType).
                setBottommostCompressionType(bottomMostCompressionType).
                setNumLevels(7).
                setCompactionPriority(CompactionPriority.MinOverlappingRatio).
                setCompactionStyle(CompactionStyle.UNIVERSAL).
            setCompactionOptionsUniversal(compactionOption).
            setMaxCompactionBytes(100 * SizeUnit.GB).
            setSoftPendingCompactionBytesLimit(100 * SizeUnit.GB).
            setHardPendingCompactionBytesLimit(256 * SizeUnit.GB).
            setLevel0FileNumCompactionTrigger(2).
            setLevel0SlowdownWritesTrigger(8).
            setLevel0StopWritesTrigger(10).
            setTargetFileSizeBase(256 * SizeUnit.MB).
            setTargetFileSizeMultiplier(2).
            setMergeOperator(new StringAppendOperator()).
            setCompactionFilterFactory(consumeQueueCompactionFilterFactory).
            setReportBgIoStats(true).
                setOptimizeFiltersForHits(true);

    }

    public static ColumnFamilyOptions createOffsetCFOptions() {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kBinarySearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch).
                setBlockSize(32 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(128 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        return columnFamilyOptions.setMaxWriteBufferNumber(4).
                setWriteBufferSize(64 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(blockBasedTableConfig).
                setMemTableConfig(new SkipListMemTableConfig()).
                setCompressionType(CompressionType.NO_COMPRESSION).
                setNumLevels(7).
                setCompactionStyle(CompactionStyle.LEVEL).
                setLevel0FileNumCompactionTrigger(2).
                setLevel0SlowdownWritesTrigger(8).
                setLevel0StopWritesTrigger(10).
                setTargetFileSizeBase(64 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMaxBytesForLevelBase(256 * SizeUnit.MB).
                setMaxBytesForLevelMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setInplaceUpdateSupport(true);
    }

    public static ColumnFamilyOptions createPopCFOptions() {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
            .setFormatVersion(5)
            .setIndexType(IndexType.kBinarySearch)
            .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
            .setDataBlockHashTableUtilRatio(0.75)
            .setBlockSize(32 * SizeUnit.KB)
            .setMetadataBlockSize(4 * SizeUnit.KB)
            .setFilterPolicy(new BloomFilter(16, false))
            .setCacheIndexAndFilterBlocks(false)
            .setCacheIndexAndFilterBlocksWithHighPriority(true)
            .setPinL0FilterAndIndexBlocksInCache(false)
            .setPinTopLevelIndexAndFilter(true)
            .setBlockCache(new LRUCache(1024 * SizeUnit.MB, 8, false))
            .setWholeKeyFiltering(true);

        CompactionOptionsUniversal compactionOption = new CompactionOptionsUniversal()
            .setSizeRatio(100)
            .setMaxSizeAmplificationPercent(25)
            .setAllowTrivialMove(true)
            .setMinMergeWidth(2)
            .setMaxMergeWidth(Integer.MAX_VALUE)
            .setStopStyle(CompactionStopStyle.CompactionStopStyleTotalSize)
            .setCompressionSizePercent(-1);

        //noinspection resource
        return new ColumnFamilyOptions()
            .setMaxWriteBufferNumber(4)
            .setWriteBufferSize(128 * SizeUnit.MB)
            .setMinWriteBufferNumberToMerge(1)
            .setTableFormatConfig(blockBasedTableConfig)
            .setMemTableConfig(new SkipListMemTableConfig())
            .setCompressionType(CompressionType.NO_COMPRESSION)
            .setBottommostCompressionType(CompressionType.NO_COMPRESSION)
            .setNumLevels(7)
            .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
            .setCompactionStyle(CompactionStyle.UNIVERSAL)
            .setCompactionOptionsUniversal(compactionOption)
            .setMaxCompactionBytes(100 * SizeUnit.GB)
            .setSoftPendingCompactionBytesLimit(100 * SizeUnit.GB)
            .setHardPendingCompactionBytesLimit(256 * SizeUnit.GB)
            .setLevel0FileNumCompactionTrigger(2)
            .setLevel0SlowdownWritesTrigger(8)
            .setLevel0StopWritesTrigger(10)
            .setTargetFileSizeBase(256 * SizeUnit.MB)
            .setTargetFileSizeMultiplier(2)
            .setMergeOperator(new StringAppendOperator())
            .setReportBgIoStats(true)
            .setOptimizeFiltersForHits(true);
    }

    /**
     * Create a rocksdb db options, the user must take care to close it after closing db.
     * @return
     */
    public static DBOptions createDBOptions() {
        //Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        // and http://gitlab.alibaba-inc.com/aloha/aloha/blob/branch_2_5_0/jstorm-core/src/main/java/com/alibaba/jstorm/cache/rocksdb/RocksDbOptionsFactory.java
        DBOptions options = new DBOptions();
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return options.
                setDbLogDir(ConfigHelper.getDBLogDir()).
                setInfoLogLevel(InfoLogLevel.INFO_LEVEL).
                setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery).
                setManualWalFlush(true).
                setCreateIfMissing(true).
                setBytesPerSync(SizeUnit.MB).
                setCreateMissingColumnFamilies(true).
                setMaxOpenFiles(-1).
                setMaxLogFileSize(SizeUnit.GB).
                setKeepLogFileNum(5).
                setMaxManifestFileSize(SizeUnit.GB).
                setAllowConcurrentMemtableWrite(false).
                setStatistics(statistics).
                setAtomicFlush(true).
                setCompactionReadaheadSize(4 * SizeUnit.MB).
                setMaxBackgroundJobs(32).
                setMaxSubcompactions(8).
                setParanoidChecks(true).
                setDelayedWriteRate(16 * SizeUnit.MB).
                setRateLimiter(new RateLimiter(100 * SizeUnit.MB)).
                setUseDirectIoForFlushAndCompaction(false).
                setUseDirectReads(false);
    }
}
