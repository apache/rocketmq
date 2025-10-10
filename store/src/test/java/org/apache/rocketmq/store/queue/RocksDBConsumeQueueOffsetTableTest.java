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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.queue.offset.OffsetEntryType;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

@RunWith(MockitoJUnitRunner.class)
public class RocksDBConsumeQueueOffsetTableTest {

    private RocksDBConsumeQueueOffsetTable offsetTable;

    @Mock
    private ConsumeQueueRocksDBStorage rocksDBStorage;

    @Mock
    private RocksDBConsumeQueueTable consumeQueueTable;

    @Mock
    private DefaultMessageStore messageStore;

    private static RocksDB db;

    private static File dbPath;

    private static String topicName;

    @BeforeClass
    public static void initDB() throws IOException, RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        TemporaryFolder tempFolder = new TemporaryFolder();
        tempFolder.create();
        dbPath = tempFolder.newFolder();

        db = RocksDB.open(dbPath.getAbsolutePath());
        StringBuilder topicBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            topicBuilder.append("topic");
        }
        topicName = topicBuilder.toString();
        byte[] topicInBytes = topicName.getBytes(StandardCharsets.UTF_8);

        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(RocksDBConsumeQueueOffsetTable.OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicInBytes.length);
        RocksDBConsumeQueueOffsetTable.buildOffsetKeyByteBuffer(keyBuffer, topicInBytes, 1, true);
        Assert.assertEquals(0, keyBuffer.position());
        Assert.assertEquals(RocksDBConsumeQueueOffsetTable.OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicInBytes.length, keyBuffer.limit());

        ByteBuffer valueBuffer = ByteBuffer.allocateDirect(Long.BYTES + Long.BYTES);
        valueBuffer.putLong(100);
        valueBuffer.putLong(2);
        valueBuffer.flip();

        try (WriteBatch writeBatch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            writeOptions.setDisableWAL(false);
            writeOptions.setSync(true);
            writeBatch.put(keyBuffer, valueBuffer);
            db.write(writeOptions, writeBatch);
        }

    }

    @AfterClass
    public static void tearDownDB() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        db.closeE();
        RocksDB.destroyDB(dbPath.getAbsolutePath(), new Options());
    }

    @Before
    public void setUp() {
        if (MixAll.isMac()) {
            return;
        }
        RocksIterator iterator = db.newIterator();
        Mockito.doReturn(iterator).when(rocksDBStorage).seekOffsetCF();
        offsetTable = new RocksDBConsumeQueueOffsetTable(consumeQueueTable, rocksDBStorage, messageStore);
    }

    /**
     * Verify forEach can expand key-buffer properly and works well for long topic names.
     *
     * @throws RocksDBException If there is an RocksDB error.
     */
    @Test
    public void testForEach() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        AtomicBoolean called = new AtomicBoolean(false);
        offsetTable.forEach(entry -> true, entry -> {
            called.set(true);
            Assert.assertEquals(topicName, entry.topic);
            Assert.assertTrue(topicName.length() > 256);
            Assert.assertEquals(1, entry.queueId);
            Assert.assertEquals(100, entry.commitLogOffset);
            Assert.assertEquals(2, entry.offset);
            Assert.assertEquals(OffsetEntryType.MAXIMUM, entry.type);
        });
        Assert.assertTrue(called.get());
    }
}
