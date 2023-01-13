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
package org.apache.rocketmq.store.tiered.metadata;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.tiered.common.TieredMessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataStoreTest {
    MessageQueue mq;
    TieredMessageStoreConfig storeConfig;
    TieredStoreMetadataStore metadataStore;

    @Before
    public void setUp() {
        mq = new MessageQueue("MetadataStoreTest", "broker", 1);
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir("/tmp/rmqut");
        metadataStore = new TieredStoreMetadataManager(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/rmqut"));
    }

    @Test
    public void testQueue() {
        QueueMetadata queueMetadata = metadataStore.getQueue(mq);
        Assert.assertNull(queueMetadata);

        queueMetadata = metadataStore.addQueue(mq, -1);
        Assert.assertEquals(queueMetadata.getMinOffset(), -1);
        Assert.assertEquals(queueMetadata.getMaxOffset(), -1);

        long currentTimeMillis = System.currentTimeMillis();
        queueMetadata.setMinOffset(0);
        queueMetadata.setMaxOffset(0);
        metadataStore.updateQueue(queueMetadata);
        queueMetadata = metadataStore.getQueue(mq);
        Assert.assertTrue(queueMetadata.getUpdateTimestamp() >= currentTimeMillis);
        Assert.assertEquals(queueMetadata.getMinOffset(), 0);
        Assert.assertEquals(queueMetadata.getMaxOffset(), 0);

        MessageQueue mq2 = new MessageQueue("MetadataStoreTest", "broker", 2);
        metadataStore.addQueue(mq2, 1);
        AtomicInteger i = new AtomicInteger(0);
        metadataStore.iterateQueue(mq.getTopic(), metadata -> {
            Assert.assertEquals(i.get(), metadata.getMinOffset());
            i.getAndIncrement();
        });
        Assert.assertEquals(i.get(), 2);

        metadataStore.deleteQueue(mq);
        queueMetadata = metadataStore.getQueue(mq);
        Assert.assertNull(queueMetadata);
    }

    @Test
    public void testTopic() {
        TopicMetadata topicMetadata = metadataStore.getTopic(mq.getTopic());
        Assert.assertNull(topicMetadata);

        metadataStore.addTopic(mq.getTopic(), 2);
        topicMetadata = metadataStore.getTopic(mq.getTopic());
        Assert.assertEquals(mq.getTopic(), topicMetadata.getTopic());
        Assert.assertEquals(topicMetadata.getStatus(), 0);
        Assert.assertEquals(topicMetadata.getReserveTime(), 2);
        Assert.assertEquals(topicMetadata.getTopicId(), 0);

        metadataStore.updateTopicStatus(mq.getTopic(), 1);
        metadataStore.updateTopicReserveTime(mq.getTopic(), 0);
        topicMetadata = metadataStore.getTopic(mq.getTopic());
        Assert.assertNotNull(topicMetadata);
        Assert.assertEquals(topicMetadata.getStatus(), 1);
        Assert.assertEquals(topicMetadata.getReserveTime(), 0);

        metadataStore.addTopic(mq.getTopic() + "1", 1);
        metadataStore.updateTopicStatus(mq.getTopic() + "1", 2);

        metadataStore.addTopic(mq.getTopic() + "2", 2);
        metadataStore.updateTopicStatus(mq.getTopic() + "2", 3);

        AtomicInteger n = new AtomicInteger();
        metadataStore.iterateTopic(metadata -> {
            long i = metadata.getReserveTime();
            Assert.assertEquals(metadata.getTopicId(), i);
            Assert.assertEquals(metadata.getStatus(), i + 1);
            if (i == 2) {
                metadataStore.deleteTopic(metadata.getTopic());
            }
            n.getAndIncrement();
        });
        Assert.assertEquals(3, n.get());

        Assert.assertNull(metadataStore.getTopic(mq.getTopic() + "2"));

        Assert.assertNotNull(metadataStore.getTopic(mq.getTopic()));
        Assert.assertNotNull(metadataStore.getTopic(mq.getTopic() + "1"));
    }

    @Test
    public void testReload() {
        TieredStoreMetadataManager metadataManager = (TieredStoreMetadataManager) metadataStore;
        metadataManager.addTopic(mq.getTopic(), 1);
        metadataManager.addQueue(mq, 2);
        metadataManager.persist();
        Assert.assertTrue(new File(metadataManager.configFilePath()).exists());

        metadataManager = new TieredStoreMetadataManager(storeConfig);
        metadataManager.load();

        TopicMetadata topicMetadata = metadataManager.getTopic(mq.getTopic());
        Assert.assertNotNull(topicMetadata);
        Assert.assertEquals(topicMetadata.getReserveTime(), 1);

        QueueMetadata queueMetadata = metadataManager.getQueue(mq);
        Assert.assertNotNull(queueMetadata);
        Assert.assertEquals(queueMetadata.getMinOffset(), 2);
    }
}
