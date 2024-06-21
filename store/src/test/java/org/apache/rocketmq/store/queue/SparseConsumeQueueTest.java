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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseConsumeQueueTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    String path;

    MessageStore defaultMessageStore;
    SparseConsumeQueue scq;

    String topic = "topic1";
    int queueId = 1;

    @Before
    public void setUp() throws IOException {
        path = tempFolder.newFolder("scq").getAbsolutePath();
        defaultMessageStore = mock(DefaultMessageStore.class);
        CommitLog commitLog = mock(CommitLog.class);
        when(defaultMessageStore.getCommitLog()).thenReturn(commitLog);
        when(commitLog.getCommitLogSize()).thenReturn(10 * 1024 * 1024);
        MessageStoreConfig config = mock(MessageStoreConfig.class);
        doReturn(config).when(defaultMessageStore).getMessageStoreConfig();
        doReturn(true).when(config).isSearchBcqByCacheEnable();
    }

    private void fillByteBuf(ByteBuffer bb, long phyOffset, long queueOffset) {
        bb.putLong(phyOffset);
        bb.putInt("size".length());
        bb.putLong("tagsCode".length());
        bb.putLong(System.currentTimeMillis());
        bb.putLong(queueOffset);
        bb.putShort((short)1);
        bb.putInt(0);
        bb.putInt(0); // 4 bytes reserved
    }

    @Test
    public void testLoad() throws IOException {
        scq = new SparseConsumeQueue(topic, queueId, path, BatchConsumeQueue.CQ_STORE_UNIT_SIZE, defaultMessageStore);

        String file1 = UtilAll.offset2FileName(111111);
        String file2 = UtilAll.offset2FileName(222222);

        long phyOffset = 10;
        long queueOffset = 1;
        ByteBuffer bb = ByteBuffer.allocate(BatchConsumeQueue.CQ_STORE_UNIT_SIZE);
        fillByteBuf(bb, phyOffset, queueOffset);
        Files.createDirectories(Paths.get(path, topic, String.valueOf(queueId)));
        Files.write(Paths.get(path, topic, String.valueOf(queueId), file1), bb.array(),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        bb.clear();
        fillByteBuf(bb, phyOffset + 1, queueOffset + 1);
        Files.write(Paths.get(path, topic, String.valueOf(queueId), file2), bb.array(),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        scq.load();
        scq.recover();
        assertEquals(scq.get(queueOffset + 1).getPos(), phyOffset + 1);
    }

    private void fillByteBufSeq(ByteBuffer bb, int circle, long basePhyOffset, long baseQueueOffset) {
        long phyOffset = basePhyOffset;
        long queueOffset = baseQueueOffset;

        for (int i = 0; i < circle; i++) {
            fillByteBuf(bb, phyOffset, queueOffset);
            phyOffset++;
            queueOffset++;
        }
    }

    @Test
    public void testSearch() throws IOException {
        int fileSize = 10 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;
        scq = new SparseConsumeQueue(topic, queueId, path, fileSize, defaultMessageStore);

        ByteBuffer bb = ByteBuffer.allocate(fileSize);
        long basePhyOffset = 101;
        long baseQueueOffset = 101;

        /* 101 -> 101 ... 110 -> 110
           201 -> 201 ... 210 -> 210
           301 -> 301 ... 310 -> 310
           ...
         */
        for (int i = 0; i < 5; i++) {
            String fileName = UtilAll.offset2FileName(i * fileSize);
            fillByteBufSeq(bb, 10, basePhyOffset, baseQueueOffset);
            Files.createDirectories(Paths.get(path, topic, String.valueOf(queueId)));
            Files.write(Paths.get(path, topic, String.valueOf(queueId), fileName), bb.array(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            basePhyOffset = i * 100 + 1;
            baseQueueOffset = i * 100 + 1;
            bb.clear();
        }

        scq.load();
        scq.recover();

        ReferredIterator<CqUnit> bufferConsumeQueue = scq.iterateFromOrNext(105);   //in the file
        assertNotNull(bufferConsumeQueue);
        assertTrue(bufferConsumeQueue.hasNext());
        assertEquals(bufferConsumeQueue.next().getQueueOffset(), 105);
        bufferConsumeQueue.release();

        bufferConsumeQueue = scq.iterateFromOrNext(120);    // in the next file
        assertNotNull(bufferConsumeQueue);
        assertTrue(bufferConsumeQueue.hasNext());
        assertEquals(bufferConsumeQueue.next().getQueueOffset(), 201);
        bufferConsumeQueue.release();

        bufferConsumeQueue = scq.iterateFromOrNext(600);       // not in the file
        assertNull(bufferConsumeQueue);
    }

    @Test
    public void testCreateFile() throws IOException {
        scq = new SparseConsumeQueue(topic, queueId, path, BatchConsumeQueue.CQ_STORE_UNIT_SIZE, defaultMessageStore);
        long physicalOffset = Math.abs(ThreadLocalRandom.current().nextLong());
        String formatName = UtilAll.offset2FileName(physicalOffset);
        scq.createFile(physicalOffset);

        assertTrue(Files.exists(Paths.get(path, topic, String.valueOf(queueId), formatName)));
        scq.putBatchMessagePositionInfo(5,4,3,2,1,(short)1);
        assertEquals(4, scq.get(1).getSize());
    }
}