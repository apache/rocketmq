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
package org.apache.rocketmq.tieredstore.metadata;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultMetadataStoreTest {

    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private MessageQueue mq0;
    private MessageQueue mq1;
    private MessageQueue mq2;
    private MessageStoreConfig storeConfig;
    private MetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        mq0 = new MessageQueue("MetadataStoreTest0", storeConfig.getBrokerName(), 0);
        mq1 = new MessageQueue("MetadataStoreTest1", storeConfig.getBrokerName(), 0);
        mq2 = new MessageQueue("MetadataStoreTest1", storeConfig.getBrokerName(), 1);
        metadataStore = new DefaultMetadataStore(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        metadataStore.destroy();
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void testQueue() {
        QueueMetadata queueMetadata = metadataStore.getQueue(mq0);
        Assert.assertNull(queueMetadata);

        queueMetadata = metadataStore.addQueue(mq0, -1);
        Assert.assertEquals(queueMetadata.getMinOffset(), -1);
        Assert.assertEquals(queueMetadata.getMaxOffset(), -1);

        long currentTimeMillis = System.currentTimeMillis();
        queueMetadata.setMinOffset(0);
        queueMetadata.setMaxOffset(0);
        metadataStore.updateQueue(queueMetadata);
        queueMetadata = metadataStore.getQueue(mq0);
        Assert.assertTrue(Objects.requireNonNull(queueMetadata).getUpdateTimestamp() >= currentTimeMillis);
        Assert.assertEquals(queueMetadata.getMinOffset(), 0);
        Assert.assertEquals(queueMetadata.getMaxOffset(), 0);

        MessageQueue mq2 = new MessageQueue(mq0.getTopic(), storeConfig.getBrokerName(), 2);
        metadataStore.addQueue(mq2, 1);
        AtomicInteger i = new AtomicInteger(0);
        metadataStore.iterateQueue(mq0.getTopic(), metadata -> {
            Assert.assertEquals(i.get(), metadata.getMinOffset());
            i.getAndIncrement();
        });
        Assert.assertEquals(i.get(), 2);

        metadataStore.deleteQueue(mq0);
        queueMetadata = metadataStore.getQueue(mq0);
        Assert.assertNull(queueMetadata);
    }

    @Test
    public void testTopic() {
        TopicMetadata topicMetadata = metadataStore.getTopic(mq0.getTopic());
        Assert.assertNull(topicMetadata);

        metadataStore.addTopic(mq0.getTopic(), 2);
        topicMetadata = metadataStore.getTopic(mq0.getTopic());
        Assert.assertEquals(mq0.getTopic(), Objects.requireNonNull(topicMetadata).getTopic());
        Assert.assertEquals(topicMetadata.getStatus(), 0);
        Assert.assertEquals(topicMetadata.getReserveTime(), 2);
        Assert.assertEquals(topicMetadata.getTopicId(), 0);

        topicMetadata.setStatus(1);
        topicMetadata.setReserveTime(0);
        metadataStore.updateTopic(topicMetadata);
        topicMetadata = metadataStore.getTopic(mq0.getTopic());
        Assert.assertNotNull(topicMetadata);
        Assert.assertEquals(topicMetadata.getStatus(), 1);
        Assert.assertEquals(topicMetadata.getReserveTime(), 0);

        String topic1 = mq0.getTopic() + "1";
        metadataStore.addTopic(topic1, 1);
        TopicMetadata topicMetadata1 = metadataStore.getTopic(topic1);
        Assert.assertNotNull(topicMetadata1);
        topicMetadata1.setStatus(2);
        metadataStore.updateTopic(topicMetadata1);

        String topic2 = mq0.getTopic() + "2";
        metadataStore.addTopic(topic2, 2);
        TopicMetadata topicMetadata2 = metadataStore.getTopic(topic2);
        Assert.assertNotNull(topicMetadata2);
        topicMetadata2.setStatus(3);
        metadataStore.updateTopic(topicMetadata2);

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
        Assert.assertNull(metadataStore.getTopic(topic2));
        Assert.assertNotNull(metadataStore.getTopic(mq0.getTopic()));
        Assert.assertNotNull(metadataStore.getTopic(topic1));
    }

    private long countFileSegment(MetadataStore metadataStore) {
        AtomicLong count = new AtomicLong();
        metadataStore.iterateFileSegment(segmentMetadata -> count.incrementAndGet());
        return count.get();
    }

    private long countFileSegment(MetadataStore metadataStore, String filePath) {
        AtomicLong count = new AtomicLong();
        metadataStore.iterateFileSegment(
            filePath, FileSegmentType.COMMIT_LOG, segmentMetadata -> count.incrementAndGet());
        return count.get();
    }

    @Test
    public void testFileSegment() {
        String filePath = MessageStoreUtil.toFilePath(mq0);
        FileSegmentMetadata segmentMetadata1 = new FileSegmentMetadata(
            filePath, 0L, FileSegmentType.COMMIT_LOG.getCode());
        metadataStore.updateFileSegment(segmentMetadata1);
        Assert.assertEquals(1L, countFileSegment(metadataStore));

        FileSegmentMetadata segmentMetadata2 = new FileSegmentMetadata(
            filePath, 100, FileSegmentType.COMMIT_LOG.getCode());
        metadataStore.updateFileSegment(segmentMetadata2);
        Assert.assertEquals(2L, countFileSegment(metadataStore));

        FileSegmentMetadata segmentMetadata = metadataStore.getFileSegment(
            filePath, FileSegmentType.COMMIT_LOG, 0L);
        Assert.assertEquals(0L, segmentMetadata.getBaseOffset());
        Assert.assertEquals(0L, segmentMetadata.getSealTimestamp());
        Assert.assertEquals(FileSegmentMetadata.STATUS_NEW, segmentMetadata.getStatus());

        segmentMetadata.markSealed();
        metadataStore.updateFileSegment(segmentMetadata);
        segmentMetadata = metadataStore.getFileSegment(
            filePath, FileSegmentType.COMMIT_LOG, segmentMetadata.getBaseOffset());
        Assert.assertEquals(FileSegmentMetadata.STATUS_SEALED, segmentMetadata.getStatus());
        Assert.assertNotEquals(0L, segmentMetadata.getSealTimestamp());

        Assert.assertEquals(2L, countFileSegment(metadataStore, filePath));
    }

    @Test
    public void testFileSegmentDelete() {
        String filePath0 = MessageStoreUtil.toFilePath(mq0);
        String filePath1 = MessageStoreUtil.toFilePath(mq1);
        for (int i = 0; i < 10; i++) {
            FileSegmentMetadata segmentMetadata = new FileSegmentMetadata(
                filePath0, i * 1000L * 1000L, FileSegmentType.COMMIT_LOG.getCode());
            metadataStore.updateFileSegment(segmentMetadata);

            segmentMetadata = new FileSegmentMetadata(
                filePath1, i * 1000L * 1000L, FileSegmentType.COMMIT_LOG.getCode());
            metadataStore.updateFileSegment(segmentMetadata);
        }
        Assert.assertEquals(20, countFileSegment(metadataStore));
        Assert.assertEquals(10, countFileSegment(metadataStore, filePath0));
        Assert.assertEquals(10, countFileSegment(metadataStore, filePath1));

        metadataStore.deleteFileSegment(filePath0, FileSegmentType.COMMIT_LOG);
        for (int i = 0; i < 5; i++) {
            metadataStore.deleteFileSegment(
                filePath1, FileSegmentType.COMMIT_LOG, i * 1000L * 1000L);
        }
        Assert.assertEquals(0L, countFileSegment(metadataStore, filePath0));
        Assert.assertEquals(5L, countFileSegment(metadataStore, filePath1));
        Assert.assertEquals(5L, countFileSegment(metadataStore));
    }

    @Test
    public void testReload() {
        DefaultMetadataStore defaultMetadataStore = (DefaultMetadataStore) metadataStore;
        defaultMetadataStore.addTopic(mq0.getTopic(), 1);
        defaultMetadataStore.addTopic(mq1.getTopic(), 2);

        defaultMetadataStore.addQueue(mq0, 2);
        defaultMetadataStore.addQueue(mq1, 4);
        defaultMetadataStore.addQueue(mq2, 8);

        String filePath0 = MessageStoreUtil.toFilePath(mq0);
        FileSegmentMetadata segmentMetadata =
            new FileSegmentMetadata(filePath0, 100, FileSegmentType.COMMIT_LOG.getCode());
        metadataStore.updateFileSegment(segmentMetadata);
        segmentMetadata =
            new FileSegmentMetadata(filePath0, 200, FileSegmentType.COMMIT_LOG.getCode());
        metadataStore.updateFileSegment(segmentMetadata);

        Assert.assertTrue(new File(defaultMetadataStore.configFilePath()).exists());

        // Reload from disk
        defaultMetadataStore = new DefaultMetadataStore(storeConfig);
        defaultMetadataStore.load();
        TopicMetadata topicMetadata = defaultMetadataStore.getTopic(mq0.getTopic());
        Assert.assertNotNull(topicMetadata);
        Assert.assertEquals(topicMetadata.getReserveTime(), 1);

        topicMetadata = defaultMetadataStore.getTopic(mq1.getTopic());
        Assert.assertNotNull(topicMetadata);
        Assert.assertEquals(topicMetadata.getReserveTime(), 2);

        QueueMetadata queueMetadata = defaultMetadataStore.getQueue(mq0);
        Assert.assertNotNull(queueMetadata);
        Assert.assertEquals(mq0, queueMetadata.getQueue());
        Assert.assertEquals(queueMetadata.getMinOffset(), 2);

        queueMetadata = defaultMetadataStore.getQueue(mq1);
        Assert.assertNotNull(queueMetadata);
        Assert.assertEquals(mq1, queueMetadata.getQueue());
        Assert.assertEquals(queueMetadata.getMinOffset(), 4);

        queueMetadata = defaultMetadataStore.getQueue(mq2);
        Assert.assertNotNull(queueMetadata);
        Assert.assertEquals(mq2, queueMetadata.getQueue());
        Assert.assertEquals(queueMetadata.getMinOffset(), 8);

        Map<Long, FileSegmentMetadata> map = new HashMap<>();
        defaultMetadataStore.iterateFileSegment(metadata -> map.put(metadata.getBaseOffset(), metadata));
        FileSegmentMetadata fileSegmentMetadata = map.get(100L);
        Assert.assertNotNull(fileSegmentMetadata);
        Assert.assertEquals(filePath0, fileSegmentMetadata.getPath());

        fileSegmentMetadata = map.get(200L);
        Assert.assertNotNull(fileSegmentMetadata);
        Assert.assertEquals(filePath0, fileSegmentMetadata.getPath());
    }

    @Test
    public void basicTest() {
        this.testTopic();
        this.testQueue();
        this.testFileSegment();

        ((DefaultMetadataStore) metadataStore).encode();
        ((DefaultMetadataStore) metadataStore).encode(false);
        ((DefaultMetadataStore) metadataStore).encode(true);
    }
}
