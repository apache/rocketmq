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
package org.apache.rocketmq.tieredstore.file;

import com.sun.jna.Platform;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredIndexFileTest {

    private final String storePath = TieredStoreTestUtil.getRandomStorePath();
    private MessageQueue mq;
    private TieredMessageStoreConfig storeConfig;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerName("IndexFileBroker");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(5);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(20);
        mq = new MessageQueue("IndexFileTest", storeConfig.getBrokerName(), 1);
        TieredStoreUtil.getMetadataStore(storeConfig);
        TieredStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void testAppendAndQuery() throws IOException, ClassNotFoundException, NoSuchMethodException {
        if (Platform.isWindows()) {
            return;
        }

        TieredFileAllocator fileQueueFactory = new TieredFileAllocator(storeConfig);
        TieredIndexFile indexFile = new TieredIndexFile(fileQueueFactory, storePath);

        indexFile.append(mq, 0, "key3", 3, 300, 1000);
        indexFile.append(mq, 0, "key2", 2, 200, 1100);
        indexFile.append(mq, 0, "key1", 1, 100, 1200);

        // do not do schedule task here
        TieredStoreExecutor.shutdown();
        List<Pair<Long, ByteBuffer>> indexList =
            indexFile.queryAsync(mq.getTopic(), "key1", 1000, 1200).join();
        Assert.assertEquals(0, indexList.size());

        // do compaction once
        TieredStoreExecutor.init();
        storeConfig.setTieredStoreIndexFileRollingIdleInterval(0);
        indexFile.doScheduleTask();
        Awaitility.await().atMost(Duration.ofSeconds(10))
            .until(() -> !indexFile.getPreMappedFile().getFile().exists());

        indexList = indexFile.queryAsync(mq.getTopic(), "key1", 1000, 1200).join();
        Assert.assertEquals(1, indexList.size());

        indexFile.destroy();
    }
}
