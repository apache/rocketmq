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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the bounded iterator based {@link RocksDBConsumeQueueTable#rangeQuery} against a real
 * RocksDB instance, including the boundary and gap behavior inherited from the old multiGet path.
 */
public class RocksDBConsumeQueueTableRangeQueryTest {

    private static final int MSG_SIZE = 100;

    private ConsumeQueueRocksDBStorage storage;
    private RocksDBConsumeQueueTable table;

    @Before
    public void setUp() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMinPhyOffset()).thenReturn(0L);
        String dbPath = Files.createTempDirectory("rocksdb-cq-range-query-").toString();
        storage = new ConsumeQueueRocksDBStorage(messageStore, dbPath);
        assertTrue(storage.start());
        table = new RocksDBConsumeQueueTable(storage, messageStore);
        table.load();
    }

    @After
    public void tearDown() {
        if (storage != null) {
            storage.shutdown();
            storage.destroy();
        }
    }

    @Test
    public void testRangeQueryReadsConsecutiveOffsetsInOrder() throws Exception {
        String topic = "topic-consecutive";
        put(topic, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertOffsets(topic, 0, 2, 4, 2, 3, 4, 5);
    }

    @Test
    public void testRangeQueryStopsAtFirstMissingOffset() throws Exception {
        String topic = "topic-gap";
        put(topic, 0, 0, 1, 3, 4, 5);

        assertOffsets(topic, 0, 0, 5, 0, 1);
    }

    @Test
    public void testRangeQueryReturnsEmptyWhenStartOffsetIsMissing() throws Exception {
        String topic = "topic-missing-start";
        put(topic, 0, 5, 6, 7, 8, 9);

        assertOffsets(topic, 0, 0, 3);
        assertOffsets(topic, 0, 10, 3);
    }

    @Test
    public void testRangeQueryStopsAtRequestedCount() throws Exception {
        String topic = "topic-count";
        put(topic, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertOffsets(topic, 0, 0, 3, 0, 1, 2);
        assertOffsets(topic, 0, 3, 2, 3, 4);
        assertOffsets(topic, 0, 8, 2, 8, 9);
    }

    @Test
    public void testRangeQueryStopsAtEndOfAvailableOffsets() throws Exception {
        String topic = "topic-tail";
        put(topic, 0, 0, 1, 2);

        assertOffsets(topic, 0, 1, 10, 1, 2);
    }

    @Test
    public void testRangeQueryDoesNotCrossQueueBoundary() throws Exception {
        String topic = "topic-queues";
        put(topic, 0, 0, 1, 2);
        // Deliberately start queue 1 at the offset the queue 0 query expects next.
        put(topic, 1, 3, 4, 5);

        assertOffsets(topic, 0, 0, 4, 0, 1, 2);
        assertOffsets(topic, 1, 3, 2, 3, 4);
    }

    @Test
    public void testRangeQueryDoesNotCrossTopicBoundary() throws Exception {
        String topicA = "topic-aaaa";
        String topicB = "topic-bbbb";
        put(topicA, 0, 0, 1, 2);
        put(topicB, 0, 0, 1, 2, 3, 4, 5);

        assertOffsets(topicA, 0, 0, 4, 0, 1, 2);
    }

    @Test
    public void testRangeQuerySupportsOffsetsLargerThanInt() throws Exception {
        String topic = "topic-large-offset";
        long first = 4_294_967_297L;
        put(topic, 0, first, first + 1, first + 2);

        assertOffsets(topic, 0, first, 2, first, first + 1);
    }

    @Test
    public void testRangeQueryWithZeroCountReturnsEmpty() throws Exception {
        String topic = "topic-zero";
        put(topic, 0, 0, 1);

        assertEquals(0, table.rangeQuery(topic, 0, 0, 0).size());
    }

    @Test
    public void testRangeQuerySingleOffset() throws Exception {
        String topic = "topic-single";
        put(topic, 0, 0, 1, 2);

        assertOffsets(topic, 0, 1, 1, 1);
        assertEquals(0, table.rangeQuery(topic, 0, 5, 1).size());
    }

    private void put(String topic, int queueId, long... offsets) throws RocksDBException {
        try (WriteBatch writeBatch = new WriteBatch()) {
            Pair<ByteBuffer, ByteBuffer> cqBBPair = RocksDBConsumeQueueTable.getCQByteBufferPair();
            for (long offset : offsets) {
                DispatchEntry entry = new DispatchEntry();
                entry.topic = topic.getBytes(StandardCharsets.UTF_8);
                entry.queueId = queueId;
                entry.queueOffset = offset;
                entry.commitLogOffset = offset * 1000L + 7;
                entry.messageSize = MSG_SIZE;
                entry.tagCode = offset;
                entry.storeTimestamp = 1_000L + offset;
                table.buildAndPutCQByteBuffer(cqBBPair, entry, writeBatch);
            }
            storage.batchPut(writeBatch);
        }
    }

    private void assertOffsets(String topic, int queueId, long startIndex, int num, long... expectedOffsets)
        throws RocksDBException {
        List<ByteBuffer> buffers = table.rangeQuery(topic, queueId, startIndex, num);
        List<Long> actualOffsets = new ArrayList<>(buffers.size());
        for (ByteBuffer buffer : buffers) {
            long phyOffset = buffer.getLong();
            int msgSize = buffer.getInt();
            long tagCode = buffer.getLong();
            assertEquals(MSG_SIZE, msgSize);
            actualOffsets.add((phyOffset - 7) / 1000L);
            assertEquals(actualOffsets.get(actualOffsets.size() - 1).longValue(), tagCode);
        }
        assertEquals(expectedOffsets.length, actualOffsets.size());
        for (int i = 0; i < expectedOffsets.length; i++) {
            assertEquals(expectedOffsets[i], actualOffsets.get(i).longValue());
        }
    }
}
