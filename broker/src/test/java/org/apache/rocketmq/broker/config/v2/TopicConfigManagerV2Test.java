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
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(value = MockitoJUnitRunner.class)
public class TopicConfigManagerV2Test {

    private MessageStoreConfig messageStoreConfig;

    private ConfigStorage configStorage;

    @Mock
    private BrokerController controller;

    @Mock
    private MessageStore messageStore;

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
        BrokerConfig brokerConfig = new BrokerConfig();
        Mockito.doReturn(brokerConfig).when(controller).getBrokerConfig();

        messageStoreConfig = new MessageStoreConfig();
        Mockito.doReturn(messageStoreConfig).when(controller).getMessageStoreConfig();
        Mockito.doReturn(messageStore).when(controller).getMessageStore();

        File configStoreDir = tf.newFolder();
        messageStoreConfig.setStorePathRootDir(configStoreDir.getAbsolutePath());

        configStorage = new ConfigStorage(messageStoreConfig);
        configStorage.start();
    }

    @Test
    public void testUpdateTopicConfig() {
        TopicConfigManagerV2 topicConfigManagerV2 = new TopicConfigManagerV2(controller, configStorage);
        topicConfigManagerV2.load();

        TopicConfig topicConfig = new TopicConfig();
        String topicName = "T1";
        topicConfig.setTopicName(topicName);
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(4);
        topicConfig.setOrder(true);
        topicConfig.setTopicSysFlag(4);
        topicConfigManagerV2.updateTopicConfig(topicConfig);

        Assert.assertTrue(configStorage.shutdown());

        topicConfigManagerV2.getTopicConfigTable().clear();

        configStorage = new ConfigStorage(messageStoreConfig);
        Assert.assertTrue(configStorage.start());
        topicConfigManagerV2 = new TopicConfigManagerV2(controller, configStorage);
        Assert.assertTrue(topicConfigManagerV2.load());

        TopicConfig loaded = topicConfigManagerV2.selectTopicConfig(topicName);
        Assert.assertNotNull(loaded);
        Assert.assertEquals(topicName, loaded.getTopicName());
        Assert.assertEquals(6, loaded.getPerm());
        Assert.assertEquals(8, loaded.getReadQueueNums());
        Assert.assertEquals(4, loaded.getWriteQueueNums());
        Assert.assertTrue(loaded.isOrder());
        Assert.assertEquals(4, loaded.getTopicSysFlag());

        Assert.assertTrue(topicConfigManagerV2.containsTopic(topicName));
    }

    @Test
    public void testRemoveTopicConfig() {
        TopicConfig topicConfig = new TopicConfig();
        String topicName = "T1";
        topicConfig.setTopicName(topicName);
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(4);
        topicConfig.setOrder(true);
        topicConfig.setTopicSysFlag(4);
        TopicConfigManagerV2 topicConfigManagerV2 = new TopicConfigManagerV2(controller, configStorage);
        topicConfigManagerV2.updateTopicConfig(topicConfig);
        topicConfigManagerV2.removeTopicConfig(topicName);
        Assert.assertFalse(topicConfigManagerV2.containsTopic(topicName));
        Assert.assertTrue(configStorage.shutdown());

        configStorage = new ConfigStorage(messageStoreConfig);
        Assert.assertTrue(configStorage.start());
        topicConfigManagerV2 = new TopicConfigManagerV2(controller, configStorage);
        Assert.assertTrue(topicConfigManagerV2.load());
        Assert.assertFalse(topicConfigManagerV2.containsTopic(topicName));
    }
}
