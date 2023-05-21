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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredFlatFileManagerTest {

    private final String storePath = TieredStoreTestUtil.getRandomStorePath();
    private TieredMessageStoreConfig storeConfig;
    private MessageQueue mq;
    private TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        mq = new MessageQueue("TieredFlatFileManagerTest", storeConfig.getBrokerName(), 0);
        metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        TieredStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyContainerManager();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void testLoadAndDestroy() {
        metadataStore.addTopic(mq.getTopic(), 0);
        metadataStore.addQueue(mq, 100);
        MessageQueue mq1 = new MessageQueue(mq.getTopic(), mq.getBrokerName(), 1);
        metadataStore.addQueue(mq1, 200);
        TieredFlatFileManager containerManager = TieredFlatFileManager.getInstance(storeConfig);
        boolean load = containerManager.load();
        Assert.assertTrue(load);

        Awaitility.await()
            .atMost(3, TimeUnit.SECONDS)
            .until(() -> containerManager.deepCopyFlatFileToList().size() == 2);

        CompositeFlatFile container = containerManager.getFlatFile(mq);
        Assert.assertNotNull(container);
        Assert.assertEquals(100, container.getDispatchOffset());

        CompositeFlatFile container1 = containerManager.getFlatFile(mq1);
        Assert.assertNotNull(container1);
        Assert.assertEquals(200, container1.getDispatchOffset());

        containerManager.destroyCompositeFile(mq);
        Assert.assertTrue(container.isClosed());
        Assert.assertNull(containerManager.getFlatFile(mq));
        Assert.assertNull(metadataStore.getQueue(mq));

        containerManager.destroy();
        Assert.assertTrue(container1.isClosed());
        Assert.assertNull(containerManager.getFlatFile(mq1));
        Assert.assertNull(metadataStore.getQueue(mq1));
    }
}
