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

package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicConfigManagerTest {
    @Mock
    private DefaultMessageStore messageStore;
    @Mock
    private BrokerController brokerController;

    private TopicConfigManager topicConfigManager;

    private static final String topic = "FooBar";
    private static final String broker1Name = "broker1";
    private static final String broker1Addr = "127.0.0.1:12345";
    private static final int queueId1 = 1;
    private static final String broker2Name = "broker2";
    private static final String broker2Addr = "127.0.0.2:12345";
    private static final int queueId2 = 2;

    @Before
    public void before() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(broker1Name);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        when(brokerController.getMessageStore()).thenReturn(messageStore);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir"));
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        this.topicConfigManager = new TopicConfigManager(brokerController);
        this.topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
    }

    @After
    public void after() throws Exception {
        if (topicConfigManager != null) {
            Files.deleteIfExists(Paths.get(topicConfigManager.configFilePath()));
        }
    }

}