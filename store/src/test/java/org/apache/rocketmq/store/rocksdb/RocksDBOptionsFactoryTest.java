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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RocksDBOptionsFactoryTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void loadRocksDB() {
        RocksDB.loadLibrary();
    }

    @Test
    public void testBottomMostCompressionType() {
        MessageStoreConfig config = new MessageStoreConfig();
        Assert.assertEquals(CompressionType.ZSTD_COMPRESSION,
            CompressionType.getCompressionType(config.getBottomMostCompressionTypeForConsumeQueueStore()));
        Assert.assertEquals(CompressionType.LZ4_COMPRESSION, CompressionType.getCompressionType("lz4"));
    }

    @Test
    public void testConsumeQueueUniversalCompactionMaxSizeAmplificationPercent() {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setRocksdbMaxSizeAmplificationPercent(50);
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(config);

        try (ColumnFamilyOptions options = RocksDBOptionsFactory.createCQCFOptions(messageStore);
            CompactionOptionsUniversal compactionOptions = options.compactionOptionsUniversal()) {
            Assert.assertEquals(50, compactionOptions.maxSizeAmplificationPercent());
        }
    }

    /**
     * The offset CF holds the max offset key that is written in the same WriteBatch as the consume queue units.
     * In-place updates are not protected by sequence-number visibility, so readers could see the new max offset
     * before the consume queue units of the same batch.
     */
    @Test
    public void testOffsetCFOptionsDisableInplaceUpdate() {
        try (ColumnFamilyOptions options = RocksDBOptionsFactory.createOffsetCFOptions()) {
            Assert.assertFalse(options.inplaceUpdateSupport());
        }
    }

    /**
     * Mirrors RocksDBConsumeQueueStore#putMessagePosition0: one WriteBatch puts a consume queue unit into the default
     * CF and the max offset into the offset CF. Once a reader sees the max offset, the unit it points to must be
     * visible as well.
     */
    @Test
    public void testOffsetCFMaxOffsetVisibleWithConsumeQueueUnit() throws Exception {
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        File dbPath = tempFolder.newFolder();

        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (ColumnFamilyOptions cqOptions = RocksDBOptionsFactory.createCQCFOptions(messageStore);
             ColumnFamilyOptions offsetOptions = RocksDBOptionsFactory.createOffsetCFOptions();
             DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
                 .setManualWalFlush(true).setAllowConcurrentMemtableWrite(false).setAtomicFlush(true);
             RocksDB db = RocksDB.open(dbOptions, dbPath.getAbsolutePath(), Arrays.asList(
                 new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cqOptions),
                 new ColumnFamilyDescriptor("offset".getBytes(), offsetOptions)), handles);
             WriteOptions writeOptions = new WriteOptions().setSync(false).setDisableWAL(true);
             ReadOptions readOptions = new ReadOptions()) {
            final ColumnFamilyHandle cqHandle = handles.get(0);
            final ColumnFamilyHandle offsetHandle = handles.get(1);
            final byte[] maxKey = "max".getBytes();
            final AtomicBoolean stop = new AtomicBoolean(false);
            final AtomicLong reads = new AtomicLong();
            final AtomicLong missing = new AtomicLong();
            final AtomicReference<Throwable> error = new AtomicReference<>();

            List<Thread> readers = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                Thread reader = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            byte[] max = db.get(offsetHandle, readOptions, maxKey);
                            if (max == null) {
                                continue;
                            }
                            reads.incrementAndGet();
                            if (db.get(cqHandle, readOptions, max) == null) {
                                missing.incrementAndGet();
                            }
                        }
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    }
                });
                readers.add(reader);
                reader.start();
            }

            try {
                long deadline = System.currentTimeMillis() + 3000;
                for (long i = 0; System.currentTimeMillis() < deadline && missing.get() == 0 && error.get() == null; i++) {
                    byte[] cqKey = ByteBuffer.allocate(Long.BYTES).putLong(i).array();
                    try (WriteBatch writeBatch = new WriteBatch()) {
                        writeBatch.put(cqHandle, cqKey, cqKey);
                        writeBatch.put(offsetHandle, maxKey, cqKey);
                        db.write(writeOptions, writeBatch);
                    }
                }
            } finally {
                stop.set(true);
                for (Thread reader : readers) {
                    reader.join();
                }
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
            }

            Assert.assertNull(error.get());
            Assert.assertTrue(reads.get() > 0);
            Assert.assertEquals("max offset visible before its consume queue unit, reads: " + reads.get(),
                0, missing.get());
        }
    }

}
