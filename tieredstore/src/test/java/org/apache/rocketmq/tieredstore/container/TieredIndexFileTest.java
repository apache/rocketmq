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
package org.apache.rocketmq.tieredstore.container;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.mock.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TieredIndexFileTest {
    MessageQueue mq;
    TieredMessageStoreConfig storeConfig;
    TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        MemoryFileSegment.checkSize = false;
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(2);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(3);
        mq = new MessageQueue("TieredIndexFileTest", storeConfig.getBrokerName(), 1);
        metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        MemoryFileSegment.checkSize = true;
        FileUtils.deleteDirectory(new File(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID()));
//        metadataStore.reLoadStore();
    }

    @Ignore
    @Test
    public void testAppendAndQuery() throws IOException, ClassNotFoundException, NoSuchMethodException {
        // skip this test on windows
        Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);

        TieredIndexFile indexFile = new TieredIndexFile(storeConfig);
        indexFile.append(mq, 0, "key3", 3, 300, 1000);
        indexFile.append(mq, 0, "key2", 2, 200, 1100);
        indexFile.append(mq, 0, "key1", 1, 100, 1200);

        Awaitility.waitAtMost(5, TimeUnit.SECONDS)
            .until(() -> {
                List<Pair<Long, ByteBuffer>> indexList = indexFile.queryAsync(mq.getTopic(), "key1", 1000, 1200).join();
                if (indexList.size() != 1) {
                    return false;
                }

                ByteBuffer indexBuffer = indexList.get(0).getValue();
                Assert.assertEquals(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE * 2, indexBuffer.remaining());

                Assert.assertEquals(1, indexBuffer.getLong(4 + 4 + 4));
                Assert.assertEquals(100, indexBuffer.getInt(4 + 4 + 4 + 8));
                Assert.assertEquals(200, indexBuffer.getInt(4 + 4 + 4 + 8 + 4));

                Assert.assertEquals(3, indexBuffer.getLong(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE + 4 + 4 + 4));
                Assert.assertEquals(300, indexBuffer.getInt(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE + 4 + 4 + 4 + 8));
                Assert.assertEquals(0, indexBuffer.getInt(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE + 4 + 4 + 4 + 8 + 4));
                return true;
            });

        indexFile.append(mq, 0, "key4", 4, 400, 1300);
        indexFile.append(mq, 0, "key4", 4, 400, 1300);
        indexFile.append(mq, 0, "key4", 4, 400, 1300);

        Awaitility.waitAtMost(5, TimeUnit.SECONDS)
            .until(() -> {
                List<Pair<Long, ByteBuffer>> indexList = indexFile.queryAsync(mq.getTopic(), "key4", 1300, 1300).join();
                if (indexList.size() != 1) {
                    return false;
                }

                ByteBuffer indexBuffer = indexList.get(0).getValue();
                Assert.assertEquals(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE * 3, indexBuffer.remaining());
                Assert.assertEquals(4, indexBuffer.getLong(4 + 4 + 4));
                Assert.assertEquals(400, indexBuffer.getInt(4 + 4 + 4 + 8));
                Assert.assertEquals(0, indexBuffer.getInt(4 + 4 + 4 + 8 + 4));
                return true;
            });

        List<Pair<Long, ByteBuffer>> indexList = indexFile.queryAsync(mq.getTopic(), "key1", 1300, 1300).join();
        Assert.assertEquals(0, indexList.size());

        indexList = indexFile.queryAsync(mq.getTopic(), "key4", 1200, 1300).join();
        Assert.assertEquals(2, indexList.size());

        ByteBuffer indexBuffer = indexList.get(0).getValue();
        Assert.assertEquals(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE * 3, indexBuffer.remaining());
        Assert.assertEquals(4, indexBuffer.getLong(4 + 4 + 4));
        Assert.assertEquals(400, indexBuffer.getInt(4 + 4 + 4 + 8));
        Assert.assertEquals(0, indexBuffer.getInt(4 + 4 + 4 + 8 + 4));

        indexBuffer = indexList.get(1).getValue();
        Assert.assertEquals(TieredIndexFile.INDEX_FILE_HASH_COMPACT_INDEX_SIZE, indexBuffer.remaining());
        Assert.assertEquals(2, indexBuffer.getLong(4 + 4 + 4));
        Assert.assertEquals(200, indexBuffer.getInt(4 + 4 + 4 + 8));
        Assert.assertEquals(100, indexBuffer.getInt(4 + 4 + 4 + 8 + 4));
    }
}
