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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.junit.Assert;
import org.junit.Test;
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
            consumerStore.scanExpiredRecords(20002, 2);
        Assert.assertEquals(2, consumerRecords.size());
        consumerStore.deleteRecords(consumerRecords);

        consumerRecords = consumerStore.scanExpiredRecords(20002, 2);
        Assert.assertEquals(1, consumerRecords.size());
        consumerStore.deleteRecords(consumerRecords);

        consumerRecords = consumerStore.scanExpiredRecords(20004, 3);
        Assert.assertEquals(2, consumerRecords.size());

        consumerStore.shutdown();
        deleteStoreDirectory(filePath);
    }
}