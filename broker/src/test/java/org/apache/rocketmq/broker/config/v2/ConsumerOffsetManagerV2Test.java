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

package org.apache.rocketmq.broker.config.v2;

import java.io.File;
import java.io.IOException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerOffsetManagerV2Test {

    private ConfigStorage configStorage;

    private ConsumerOffsetManagerV2 consumerOffsetManagerV2;

    @Mock
    private BrokerController controller;

    private MessageStoreConfig messageStoreConfig;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @After
    public void cleanUp() {
        if (null != configStorage) {
            configStorage.shutdown();
        }
    }

    @Before
    public void setUp() throws IOException {
        Assume.assumeFalse(MixAll.isMac());
        BrokerConfig brokerConfig = new BrokerConfig();
        Mockito.doReturn(brokerConfig).when(controller).getBrokerConfig();

        File configStoreDir = tf.newFolder();
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(configStoreDir.getAbsolutePath());
        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        consumerOffsetManagerV2 = new ConsumerOffsetManagerV2(controller, configStorage);
    }

    /**
     * Verify consumer offset can survive restarts
     */
    @Test
    public void testCommitOffset_Standard() {
        Assume.assumeFalse(MixAll.isMac());
        Assert.assertTrue(consumerOffsetManagerV2.load());

        String clientHost = "localhost";
        String topic = "T1";
        String group = "G0";
        int queueId = 1;
        long queueOffset = 100;
        consumerOffsetManagerV2.commitOffset(clientHost, group, topic, queueId, queueOffset);
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic, queueId));

        configStorage.shutdown();
        consumerOffsetManagerV2.getOffsetTable().clear();
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic, queueId));

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        consumerOffsetManagerV2 = new ConsumerOffsetManagerV2(controller, configStorage);
        consumerOffsetManagerV2.load();
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
    }

    /**
     * Verify commit offset can survive config store restart
     */
    @Test
    public void testCommitOffset_LMQ() {
        Assume.assumeFalse(MixAll.isMac());
        Assert.assertTrue(consumerOffsetManagerV2.load());

        String clientHost = "localhost";
        String topic = MixAll.LMQ_PREFIX + "T1";
        String group = "G0";
        int queueId = 1;
        long queueOffset = 100;
        consumerOffsetManagerV2.commitOffset(clientHost, group, topic, queueId, queueOffset);
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic, queueId));

        configStorage.shutdown();

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        consumerOffsetManagerV2 = new ConsumerOffsetManagerV2(controller, configStorage);
        consumerOffsetManagerV2.load();
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
    }


    /**
     * Verify commit offset can survive config store restart
     */
    @Test
    public void testCommitPullOffset_LMQ() {
        Assume.assumeFalse(MixAll.isMac());
        Assert.assertTrue(consumerOffsetManagerV2.load());

        String clientHost = "localhost";
        String topic = MixAll.LMQ_PREFIX + "T1";
        String group = "G0";
        int queueId = 1;
        long queueOffset = 100;
        consumerOffsetManagerV2.commitPullOffset(clientHost, group, topic, queueId, queueOffset);
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryPullOffset(group, topic, queueId));

        configStorage.shutdown();

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        consumerOffsetManagerV2 = new ConsumerOffsetManagerV2(controller, configStorage);
        consumerOffsetManagerV2.load();
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryPullOffset(group, topic, queueId));
    }

    /**
     * Verify commit offset can survive config store restart
     */
    @Test
    public void testRemoveByTopicAtGroup() {
        Assume.assumeFalse(MixAll.isMac());
        Assert.assertTrue(consumerOffsetManagerV2.load());

        String clientHost = "localhost";
        String topic = MixAll.LMQ_PREFIX + "T1";
        String topic2 = MixAll.LMQ_PREFIX + "T2";
        String group = "G0";
        int queueId = 1;
        long queueOffset = 100;
        consumerOffsetManagerV2.commitOffset(clientHost, group, topic, queueId, queueOffset);
        consumerOffsetManagerV2.commitOffset(clientHost, group, topic2, queueId, queueOffset);
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic2, queueId));

        consumerOffsetManagerV2.removeConsumerOffset(topic + ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR + group);
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic2, queueId));

        configStorage.shutdown();

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        consumerOffsetManagerV2 = new ConsumerOffsetManagerV2(controller, configStorage);
        consumerOffsetManagerV2.load();
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic2, queueId));
    }

    /**
     * Verify commit offset can survive config store restart
     */
    @Test
    public void testRemoveByGroup() {
        Assume.assumeFalse(MixAll.isMac());
        Assert.assertTrue(consumerOffsetManagerV2.load());

        String clientHost = "localhost";
        String topic = MixAll.LMQ_PREFIX + "T1";
        String topic2 = MixAll.LMQ_PREFIX + "T2";
        String group = "G0";
        int queueId = 1;
        long queueOffset = 100;
        consumerOffsetManagerV2.commitOffset(clientHost, group, topic, queueId, queueOffset);
        consumerOffsetManagerV2.commitOffset(clientHost, group, topic2, queueId, queueOffset);
        Assert.assertEquals(queueOffset, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
        consumerOffsetManagerV2.removeOffset(group);
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic2, queueId));

        configStorage.shutdown();

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
        consumerOffsetManagerV2 = new ConsumerOffsetManagerV2(controller, configStorage);
        consumerOffsetManagerV2.load();
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic, queueId));
        Assert.assertEquals(-1L, consumerOffsetManagerV2.queryOffset(group, topic2, queueId));
    }

}
