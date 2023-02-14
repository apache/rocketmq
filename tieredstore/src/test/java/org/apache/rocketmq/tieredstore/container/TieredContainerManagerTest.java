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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredContainerManagerTest {
    TieredMessageStoreConfig storeConfig;
    MessageQueue mq;
    TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.mock.MemoryFileSegment");
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        mq = new MessageQueue("TieredContainerManagerTest", storeConfig.getBrokerName(), 0);
        metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID()));
        TieredStoreUtil.getMetadataStore(storeConfig).destroy();
        TieredContainerManager.getInstance(storeConfig).cleanup();
    }


    @Test
    public void testLoadAndDestroy() {
        metadataStore.addTopic(mq.getTopic(), 0);
        metadataStore.addQueue(mq, 100);
        MessageQueue mq1 = new MessageQueue(mq.getTopic(), mq.getBrokerName(), 1);
        metadataStore.addQueue(mq1, 200);
        TieredContainerManager containerManager = TieredContainerManager.getInstance(storeConfig);
        boolean load = containerManager.load();
        Assert.assertTrue(load);

        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> containerManager.getAllMQContainer().size() == 2);

        TieredMessageQueueContainer container = containerManager.getMQContainer(mq);
        Assert.assertNotNull(container);
        Assert.assertEquals(100, container.getDispatchOffset());

        TieredMessageQueueContainer container1 = containerManager.getMQContainer(mq1);
        Assert.assertNotNull(container1);
        Assert.assertEquals(200, container1.getDispatchOffset());

        containerManager.destroyContainer(mq);
        Assert.assertTrue(container.isClosed());
        Assert.assertNull(containerManager.getMQContainer(mq));
        Assert.assertNull(metadataStore.getQueue(mq));

        containerManager.destroy();
        Assert.assertTrue(container1.isClosed());
        Assert.assertNull(containerManager.getMQContainer(mq1));
        Assert.assertNull(metadataStore.getQueue(mq1));
    }
}
