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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicRemappingDetailWrapper;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicQueueMappingManagerTest {
    @Mock
    private BrokerController brokerController;
    private static final String BROKER1_NAME = "broker1";

    @Before
    public void before() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(BROKER1_NAME);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir"));
        messageStoreConfig.setDeleteWhen("01;02;03;04;05;06;07;08;09;10;11;12;13;14;15;16;17;18;19;20;21;22;23;00");
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
    }


    private void delete(TopicQueueMappingManager topicQueueMappingManager) throws Exception {
        if (topicQueueMappingManager == null) {
            return;
        }
        Files.deleteIfExists(Paths.get(topicQueueMappingManager.configFilePath()));
        Files.deleteIfExists(Paths.get(topicQueueMappingManager.configFilePath() + ".bak"));


    }

    @Test
    public void testEncodeDecode() throws Exception {
        Map<String, TopicQueueMappingDetail> mappingDetailMap = new HashMap<>();
        TopicQueueMappingManager topicQueueMappingManager = null;
        Set<String> brokers = new HashSet<>();
        brokers.add(BROKER1_NAME);
        {
            for (int i = 0; i < 10; i++) {
                String topic = UUID.randomUUID().toString();
                int queueNum = 10;
                TopicRemappingDetailWrapper topicRemappingDetailWrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, brokers, new HashMap<>());
                Assert.assertEquals(1, topicRemappingDetailWrapper.getBrokerConfigMap().size());
                TopicQueueMappingDetail topicQueueMappingDetail  = topicRemappingDetailWrapper.getBrokerConfigMap().values().iterator().next().getMappingDetail();
                Assert.assertEquals(queueNum, topicQueueMappingDetail.getHostedQueues().size());
                mappingDetailMap.put(topic, topicQueueMappingDetail);
            }
        }

        {
            topicQueueMappingManager = new TopicQueueMappingManager(brokerController);
            Assert.assertTrue(topicQueueMappingManager.load());
            Assert.assertEquals(0, topicQueueMappingManager.getTopicQueueMappingTable().size());
            for (TopicQueueMappingDetail mappingDetail : mappingDetailMap.values()) {
                for (int i = 0; i < 10; i++) {
                    topicQueueMappingManager.updateTopicQueueMapping(mappingDetail, false, false, true);
                }
            }
            topicQueueMappingManager.persist();
        }

        {
            topicQueueMappingManager = new TopicQueueMappingManager(brokerController);
            Assert.assertTrue(topicQueueMappingManager.load());
            Assert.assertEquals(mappingDetailMap.size(), topicQueueMappingManager.getTopicQueueMappingTable().size());
            for (TopicQueueMappingDetail topicQueueMappingDetail: topicQueueMappingManager.getTopicQueueMappingTable().values()) {
                Assert.assertEquals(topicQueueMappingDetail, mappingDetailMap.get(topicQueueMappingDetail.getTopic()));
            }
        }
        delete(topicQueueMappingManager);
    }
}
