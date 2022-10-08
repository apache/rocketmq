/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.transaction;

import java.time.Duration;
import java.util.Random;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TransactionDataManagerTest extends InitConfigAndLoggerTest {
    private static final String PRODUCER_GROUP = "producerGroup";
    private static final Random RANDOM = new Random();
    private TransactionDataManager transactionDataManager;

    @Before
    public void before() throws Throwable {
        super.before();
        this.transactionDataManager = new TransactionDataManager();
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void testAddAndRemove() {
        TransactionData transactionData1 = createTransactionData();
        TransactionData transactionData2 = createTransactionData(transactionData1.getTransactionId());
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, transactionData1.getTransactionId(), transactionData1);
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, transactionData1.getTransactionId(), transactionData2);

        assertEquals(1, this.transactionDataManager.transactionIdDataMap.size());
        assertEquals(2, this.transactionDataManager.transactionIdDataMap.get(
            transactionDataManager.buildKey(PRODUCER_GROUP, transactionData1.getTransactionId())).size());

        this.transactionDataManager.removeTransactionData(PRODUCER_GROUP, transactionData1.getTransactionId(), transactionData1);
        assertEquals(1, this.transactionDataManager.transactionIdDataMap.size());
        this.transactionDataManager.removeTransactionData(PRODUCER_GROUP, transactionData1.getTransactionId(), transactionData2);
        assertEquals(0, this.transactionDataManager.transactionIdDataMap.size());
    }

    @Test
    public void testPoll() {
        String txId = MessageClientIDSetter.createUniqID();
        TransactionData transactionData1 = createTransactionData(txId, System.currentTimeMillis() - Duration.ofMinutes(2).toMillis());
        TransactionData transactionData2 = createTransactionData(txId);

        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, txId, transactionData1);
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, txId, transactionData2);

        TransactionData resTransactionData = this.transactionDataManager.pollNoExpireTransactionData(PRODUCER_GROUP, txId);
        assertSame(transactionData2, resTransactionData);
        assertNull(this.transactionDataManager.pollNoExpireTransactionData(PRODUCER_GROUP, txId));
    }

    @Test
    public void testCleanExpire() {
        String txId = MessageClientIDSetter.createUniqID();
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, txId,
            createTransactionData(txId, System.currentTimeMillis(), Duration.ofMillis(100).toMillis()));
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, txId,
            createTransactionData(txId, System.currentTimeMillis(), Duration.ofMillis(500).toMillis()));

        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, MessageClientIDSetter.createUniqID(),
            createTransactionData(txId, System.currentTimeMillis(), Duration.ofMillis(1000).toMillis()));

        await().atMost(Duration.ofSeconds(2)).until(() -> {
            this.transactionDataManager.cleanExpireTransactionData();
            return this.transactionDataManager.transactionIdDataMap.isEmpty();
        });
    }

    @Test
    public void testWaitTransactionDataClear() throws InterruptedException {
        String txId = MessageClientIDSetter.createUniqID();
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, txId,
            createTransactionData(txId, System.currentTimeMillis(), Duration.ofMillis(100).toMillis()));
        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, txId,
            createTransactionData(txId, System.currentTimeMillis(), Duration.ofMillis(500).toMillis()));

        this.transactionDataManager.addTransactionData(PRODUCER_GROUP, MessageClientIDSetter.createUniqID(),
            createTransactionData(txId, System.currentTimeMillis(), Duration.ofMillis(1000).toMillis()));

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.transactionDataManager.waitTransactionDataClear();
        stopWatch.stop();
        assertTrue(Math.abs(stopWatch.getTime() - 1000) <= 50);
    }

    private static TransactionData createTransactionData() {
        return createTransactionData(MessageClientIDSetter.createUniqID());
    }

    private static TransactionData createTransactionData(String txId) {
        return createTransactionData(txId, System.currentTimeMillis());
    }

    private static TransactionData createTransactionData(String txId, long checkTimestamp) {
        return createTransactionData(txId, checkTimestamp, Duration.ofMinutes(1).toMillis());
    }

    private static TransactionData createTransactionData(String txId, long checkTimestamp, long checkImmunityTime) {
        return new TransactionData(
            "brokerName",
            RANDOM.nextLong(),
            RANDOM.nextLong(),
            txId,
            checkTimestamp,
            checkImmunityTime
        );
    }
}