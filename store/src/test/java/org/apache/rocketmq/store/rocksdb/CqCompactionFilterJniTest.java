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
import java.nio.file.Files;
import java.util.UUID;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.WriteBatch;

public class CqCompactionFilterJniTest {

    private static final int TOPIC_COUNT = 100;
    private static final int BATCH_SIZE = 100_000;
    private static final int MSG_SIZE = 1000;

    private static final byte CTRL_1 = '\u0001';
    private ConsumeQueueRocksDBStorage storage;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("CqCompactionFilterJni native library must be loaded", CqCompactionFilterJni.isLoaded());
        String dbPath = Files.createTempDirectory("rocksdb-cq-compaction-" + UUID.randomUUID()).toString();
        MessageStore mockStore = Mockito.mock(MessageStore.class);
        Mockito.when(mockStore.getMinPhyOffset()).thenReturn(0L);
        Mockito.when(mockStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        storage = new ConsumeQueueRocksDBStorage(mockStore, dbPath);
    }

    @After
    public void tearDown() {
        if (storage != null) {
            storage.shutdown();
            storage.destroy();
        }
    }

    @Test
    public void testCreateAndSetFilter() {
        Assert.assertTrue("Native library should be loaded", CqCompactionFilterJni.isLoaded());

        long ptr = CqCompactionFilterJni.createNativeFilter0();
        Assert.assertTrue("Native filter pointer should be non-zero", ptr != 0);

        CqCompactionFilterJni.setMinPhyOffset0(ptr, 1000);
        CqCompactionFilterJni.setMinPhyOffset0(ptr, Long.MAX_VALUE);

        try (ColumnFamilyOptions options = new ColumnFamilyOptions()) {
            CqCompactionFilterJni.setNativeFilter(options, ptr);
        }
    }

    @Test
    public void testCompactionFilter_small() throws Exception {
        runCompactionTest(1_000_000);
    }

    @Test
    public void testCompactionFilter_large() throws Exception {
        runCompactionTest(10_000_000);
    }

    private void runCompactionTest(int totalEntries) throws Exception {
        long start = System.currentTimeMillis();
        boolean result = storage.start();
        if (!result) {
            System.err.println("storage.start() returned false. Check ERROR logs above for details.");
        }
        Assert.assertTrue("ConsumeQueueRocksDBStorage failed to start", result);
        log("Startup took %d ms", System.currentTimeMillis() - start);

        // Phase 1: Write entries
        start = System.currentTimeMillis();
        writeEntries(totalEntries);
        long writeTime = System.currentTimeMillis() - start;
        log("Wrote %d entries in %d ms (%.0f entries/sec)", totalEntries, writeTime, totalEntries * 1000.0 / writeTime);

        // Phase 2: Count entries before compaction
        start = System.currentTimeMillis();
        long countBefore = storage.countEntries();
        long countTime = System.currentTimeMillis() - start;
        log("Count before compaction: %d (took %d ms)", countBefore, countTime);
        Assert.assertEquals("Entry count should match total written", totalEntries, countBefore);

        // Flush memtables to SST files so compaction has something to process
        start = System.currentTimeMillis();
        storage.flushAll();
        log("Flush took %d ms", System.currentTimeMillis() - start);

        // Phase 3: Set minPhyOffset at midpoint and trigger compaction
        long minPhyOffset = (long) (totalEntries / 2.0) * MSG_SIZE;
        start = System.currentTimeMillis();
        storage.triggerCompactionSync(minPhyOffset);
        long compactTime = System.currentTimeMillis() - start;
        log("Compaction with minPhyOffset=%d took %d ms", minPhyOffset, compactTime);

        // Phase 4: Count entries after compaction
        start = System.currentTimeMillis();
        long countAfter = storage.countEntries();
        countTime = System.currentTimeMillis() - start;
        log("Count after compaction: %d (took %d ms)", countAfter, countTime);

        // Verify: approximately half the entries should remain
        long expectedSurvivors = totalEntries - totalEntries / 2;
        long tolerance = Math.max(expectedSurvivors / 100, 100);
        Assert.assertTrue(
            "Expected ~" + expectedSurvivors + " entries after compaction, but got " + countAfter,
            countAfter >= expectedSurvivors - tolerance && countAfter <= expectedSurvivors + tolerance
        );

        log("Test passed: %d -> %d entries (expected ~%d)", totalEntries, countAfter, expectedSurvivors);
    }

    private void writeEntries(int totalEntries) throws Exception {
        int entriesPerTopic = totalEntries / TOPIC_COUNT;

        for (int t = 0; t < TOPIC_COUNT; t++) {
            String topic = "test-topic-" + t;
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            int queueId = 0;

            try (WriteBatch batch = new WriteBatch()) {
                for (int i = 0; i < entriesPerTopic; i++) {
                    int globalIndex = t * entriesPerTopic + i;

                    // Key: [topic_len:4][CTRL_1][topic][CTRL_1][queue_id:4][CTRL_1][cq_offset:8]
                    int keyLen = Integer.BYTES + 1 + topicBytes.length + 1 + Integer.BYTES + 1 + Long.BYTES;
                    ByteBuffer keyBB = ByteBuffer.allocate(keyLen);
                    keyBB.putInt(topicBytes.length)
                        .put(CTRL_1)
                        .put(topicBytes)
                        .put(CTRL_1)
                        .putInt(queueId)
                        .put(CTRL_1)
                        .putLong(i);

                    // Value: [phy_offset:8][msg_size:4][tags_code:8][store_timestamp:8] (28 bytes)
                    long phyOffset = (long) globalIndex * MSG_SIZE;
                    ByteBuffer valueBB = ByteBuffer.allocate(28);
                    valueBB.putLong(phyOffset)
                        .putInt(MSG_SIZE)
                        .putLong(0)
                        .putLong(System.currentTimeMillis());

                    batch.put(storage.getDefaultCFHandle(), keyBB.array(), valueBB.array());

                    if ((i + 1) % BATCH_SIZE == 0) {
                        storage.batchPut(batch);
                    }
                }
                if (entriesPerTopic % BATCH_SIZE != 0) {
                    storage.batchPut(batch);
                }
            }
        }
    }

    private void log(String format, Object... args) {
        System.out.printf("[CqCompactionFilterJniTest] " + format + "%n", args);
    }
}
