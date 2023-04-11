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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class ConsumeQueueStoreTest extends QueueTestBase {
    private MessageStore messageStore;
    private ConcurrentMap<String, TopicConfig> topicConfigTableMap;



    @Before
    public void init() throws Exception {
        this.topicConfigTableMap = new ConcurrentHashMap<>();
        messageStore = createMessageStore(null, true, topicConfigTableMap);
        messageStore.load();
        messageStore.start();
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    @Test
    public void testLoadConsumeQueuesWithWrongAttribute() {
        String normalTopic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig> topicConfigTable = createTopicConfigTable(normalTopic, CQType.SimpleCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        for (int i = 0; i < 10; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(normalTopic, -1));
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        // simulate delete topic but with files left.
        this.topicConfigTableMap.clear();

        topicConfigTable = createTopicConfigTable(normalTopic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> messageStore.getQueueStore().load());
        Assert.assertTrue(runtimeException.getMessage().endsWith("should be SimpleCQ, but is BatchCQ"));
    }

    @Test
    public void testLoadBatchConsumeQueuesWithWrongAttribute() {
        String batchTopic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(batchTopic, CQType.BatchCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);

        for (int i = 0; i < 10; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(batchTopic, 10));
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        // simulate delete topic but with files left.
        this.topicConfigTableMap.clear();

        topicConfigTable = createTopicConfigTable(batchTopic, CQType.SimpleCQ);
        this.topicConfigTableMap.putAll(topicConfigTable);
        messageStore.shutdown();

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> messageStore.getQueueStore().load());
        Assert.assertTrue(runtimeException.getMessage().endsWith("should be BatchCQ, but is SimpleCQ"));
    }

}
