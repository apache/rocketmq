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
package org.apache.rocketmq.broker.pop;

import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopConsumerRocksdbStoreTest {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final String CONSUMER_STORE_PATH = "consumer_rocksdb";

    public static String getRandomStorePath() {
        return Paths.get(System.getProperty("user.home"), "store_test", CONSUMER_STORE_PATH,
            UUID.randomUUID().toString().replace("-", "").toUpperCase().substring(0, 16)).toString();
    }

    public static void deleteStoreDirectory(String storePath) {
        try {
            FileUtils.deleteDirectory(new File(storePath));
        } catch (IOException e) {
            log.error("Delete store directory failed, filePath: {}", storePath, e);
        }
    }

    public static PopConsumerRecord getConsumerRecord() {
        return new PopConsumerRecord(1L, "GroupTest", "TopicTest", 2,
            PopConsumerRecord.RetryType.NORMAL_TOPIC.getCode(), TimeUnit.SECONDS.toMillis(20), 100L, "AttemptId");
    }

    @Test
    public void rocksdbStoreWriteDeleteTest() {
        Assume.assumeFalse(MixAll.isMac());
        String filePath = getRandomStorePath();
        PopConsumerKVStore consumerStore = new PopConsumerRocksdbStore(filePath);
        Assert.assertEquals(filePath, consumerStore.getFilePath());

        consumerStore.start();
        consumerStore.writeRecords(IntStream.range(0, 3).boxed()
            .flatMap(i ->
                IntStream.range(0, 5).mapToObj(j -> {
                    PopConsumerRecord consumerRecord = getConsumerRecord();
                    consumerRecord.setPopTime(j);
                    consumerRecord.setQueueId(i);
                    consumerRecord.setOffset(100L + j);
                    return consumerRecord;
                })
            )
            .collect(Collectors.toList()));
        consumerStore.deleteRecords(IntStream.range(0, 2).boxed()
            .flatMap(i ->
                IntStream.range(0, 5).mapToObj(j -> {
                    PopConsumerRecord consumerRecord = getConsumerRecord();
                    consumerRecord.setPopTime(j);
                    consumerRecord.setQueueId(i);
                    consumerRecord.setOffset(100L + j);
                    return consumerRecord;
                })
            )
            .collect(Collectors.toList()));

        List<PopConsumerRecord> consumerRecords =
            consumerStore.scanExpiredRecords(0, 20002, 2);
        Assert.assertEquals(2, consumerRecords.size());
        consumerStore.deleteRecords(consumerRecords);

        consumerRecords = consumerStore.scanExpiredRecords(0, 20003, 2);
        Assert.assertEquals(1, consumerRecords.size());
        consumerStore.deleteRecords(consumerRecords);

        consumerRecords = consumerStore.scanExpiredRecords(0, 20005, 3);
        Assert.assertEquals(2, consumerRecords.size());

        consumerStore.shutdown();
        deleteStoreDirectory(filePath);
    }

    private long getDirectorySizeRecursive(File directory) {
        long size = 0;
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    size += file.length();
                } else if (file.isDirectory()) {
                    size += getDirectorySizeRecursive(file);
                }
            }
        }
        return size;
    }

    @Test
    @Ignore
    @SuppressWarnings("ConstantValue")
    public void tombstoneDeletionTest() throws IllegalAccessException, NoSuchFieldException {
        Assume.assumeFalse(MixAll.isMac());
        PopConsumerRocksdbStore rocksdbStore = new PopConsumerRocksdbStore(getRandomStorePath());
        rocksdbStore.start();

        int iterCount = 1000 * 1000;
        boolean useSeekFirstDelete = false;
        Field dbField = AbstractRocksDBStorage.class.getDeclaredField("db");
        dbField.setAccessible(true);
        RocksDB rocksDB = (RocksDB) dbField.get(rocksdbStore);

        long currentTime = 0L;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < iterCount; i++) {
            List<PopConsumerRecord> records = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                PopConsumerRecord record = getConsumerRecord();
                record.setPopTime((long) i * iterCount + j);
                record.setGroupId("GroupTest");
                record.setTopicId("TopicTest");
                record.setQueueId(i % 10);
                record.setRetryFlag(0);
                record.setInvisibleTime(TimeUnit.SECONDS.toMillis(30));
                record.setOffset(i);
                records.add(record);
            }
            rocksdbStore.writeRecords(records);

            long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            List<PopConsumerRecord> deleteList = new ArrayList<>();
            if (useSeekFirstDelete) {
                try (RocksIterator iterator = rocksDB.newIterator(rocksdbStore.columnFamilyHandle)) {
                    iterator.seekToFirst();
                    if (i % 10 == 0) {
                        long fileSize = getDirectorySizeRecursive(new File(rocksdbStore.getFilePath()));
                        log.info("DirectorySize={}, Cost={}ms",
                            MessageStoreUtil.toHumanReadable(fileSize), stopwatch.elapsed(TimeUnit.MILLISECONDS) - start);
                    }
                    while (iterator.isValid() && deleteList.size() < 1024) {
                        deleteList.add(PopConsumerRecord.decode(iterator.value()));
                        iterator.next();
                    }
                }
            } else {
                long upper = System.currentTimeMillis();
                deleteList = rocksdbStore.scanExpiredRecords(currentTime, upper, 800);
                if (!deleteList.isEmpty()) {
                    currentTime = deleteList.get(deleteList.size() - 1).getVisibilityTimeout();
                }
                long scanCost = stopwatch.elapsed(TimeUnit.MILLISECONDS) - start;
                if (i % 100 == 0) {
                    long fileSize = getDirectorySizeRecursive(new File(rocksdbStore.getFilePath()));
                    long seekTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                    try (RocksIterator iterator = rocksDB.newIterator(rocksdbStore.columnFamilyHandle)) {
                        iterator.seekToFirst();
                    }
                    log.info("DirectorySize={}, Cost={}ms, SeekFirstCost={}ms", MessageStoreUtil.toHumanReadable(fileSize),
                        scanCost, stopwatch.elapsed(TimeUnit.MILLISECONDS) - seekTime);
                }
            }
            rocksdbStore.deleteRecords(deleteList);
        }
        rocksdbStore.shutdown();
        deleteStoreDirectory(rocksdbStore.getFilePath());
    }
}