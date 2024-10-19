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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
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

public class ConfigHelper {
    public static ColumnFamilyOptions createConfigOptions() {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().
            setFormatVersion(5).
            setIndexType(IndexType.kBinarySearch).
            setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch).
            setBlockSize(32 * SizeUnit.KB).
            setFilterPolicy(new BloomFilter(16, false)).
            // Indicating if we'd put index/filter blocks to the block cache.
                setCacheIndexAndFilterBlocks(false).
            setCacheIndexAndFilterBlocksWithHighPriority(true).
            setPinL0FilterAndIndexBlocksInCache(false).
            setPinTopLevelIndexAndFilter(true).
            setBlockCache(new LRUCache(4 * SizeUnit.MB, 8, false)).
            setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        return options.setMaxWriteBufferNumber(2).
            // MemTable size, MemTable(cache) -> immutable MemTable(cache) -> SST(disk)
                setWriteBufferSize(8 * SizeUnit.MB).
            setMinWriteBufferNumberToMerge(1).
            setTableFormatConfig(blockBasedTableConfig).
            setMemTableConfig(new SkipListMemTableConfig()).
            setCompressionType(CompressionType.NO_COMPRESSION).
            setNumLevels(7).
            setCompactionStyle(CompactionStyle.LEVEL).
            setLevel0FileNumCompactionTrigger(4).
            setLevel0SlowdownWritesTrigger(8).
            setLevel0StopWritesTrigger(12).
            // The target file size for compaction.
                setTargetFileSizeBase(64 * SizeUnit.MB).
            setTargetFileSizeMultiplier(2).
            // The upper-bound of the total size of L1 files in bytes
                setMaxBytesForLevelBase(256 * SizeUnit.MB).
            setMaxBytesForLevelMultiplier(2).
            setMergeOperator(new StringAppendOperator()).
            setInplaceUpdateSupport(true);
    }

    public static DBOptions createConfigDBOptions() {
        //Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        // and http://gitlab.alibaba-inc.com/aloha/aloha/blob/branch_2_5_0/jstorm-core/src/main/java/com/alibaba/jstorm/cache/rocksdb/RocksDbOptionsFactory.java
        DBOptions options = new DBOptions();
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return options.
            setDbLogDir(getDBLogDir()).
            setInfoLogLevel(InfoLogLevel.INFO_LEVEL).
            setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords).
            setManualWalFlush(true).
            setMaxTotalWalSize(500 * SizeUnit.MB).
            setWalSizeLimitMB(0).
            setWalTtlSeconds(0).
            setCreateIfMissing(true).
            setCreateMissingColumnFamilies(true).
            setMaxOpenFiles(-1).
            setMaxLogFileSize(SizeUnit.GB).
            setKeepLogFileNum(5).
            setMaxManifestFileSize(SizeUnit.GB).
            setAllowConcurrentMemtableWrite(false).
            setStatistics(statistics).
            setStatsDumpPeriodSec(600).
            setAtomicFlush(true).
            setMaxBackgroundJobs(32).
            setMaxSubcompactions(4).
            setParanoidChecks(true).
            setDelayedWriteRate(16 * SizeUnit.MB).
            setRateLimiter(new RateLimiter(100 * SizeUnit.MB)).
            setUseDirectIoForFlushAndCompaction(true).
            setUseDirectReads(true);
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
}
