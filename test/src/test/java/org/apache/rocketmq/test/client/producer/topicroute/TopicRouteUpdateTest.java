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
package org.apache.rocketmq.test.client.producer.topicroute;


import com.google.common.collect.Maps;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.log4j.Logger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.namesrv.routeinfo.TopicRouteNotifier;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.test.util.RandomUtil;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * test when topic route changed
 */
public class TopicRouteUpdateTest extends BaseConf {
    private static final Logger logger = Logger.getLogger(TopicRouteUpdateTest.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @After
    public void tearDown() {
        shutdown();
    }

    private void resetProducerAndTopic() {
        if (topic == null) {
            topic = initTopic();
            logger.info(String.format("topic is %s", topic));
        }
        if (producer == null) {
            producer = getProducer(nsAddr, topic, 30000);
        }
    }

    @Test
    public void modifyTopicQueueNumAndCheck() throws Exception {
        // if busy, then skip this test case
        if (isCurrentSystemBusy()) {
            logger.warn("system busy, skip TopicRouteUpdateTest test");
            return;
        }

        // check queue num changed
        checkModifyQueueNum();
        // check broker shutdown
        checkBrokerStartAndShutdown();
    }

    private void checkModifyQueueNum() throws Exception {
        resetProducerAndTopic();
        // wait for the ready
        Thread.sleep(8000);

        producer.send(20);
        for (int queueNum = 100; queueNum < 105; queueNum++) {
            modifyQueueNumAndCheck(queueNum);
        }
    }

    private void checkBrokerStartAndShutdown() throws Exception {
        resetProducerAndTopic();
        producer.send(20);
        Thread.sleep(2000);

        shutdownBrokerAndCheck(brokerController1);
        shutdownBrokerAndCheck(brokerController2);

        addNewBrokerAndCheck(IntegrationTestBase.createAndStartBroker(nsAddr));
        addNewBrokerAndCheck(IntegrationTestBase.createAndStartBroker(nsAddr));
    }

    private void addNewBrokerAndCheck(BrokerController newBrokerController) throws Exception {
        String brokerName = newBrokerController.getBrokerConfig().getBrokerName();
        TopicPublishInfo topicPublishInfo = queryTopicPublishInfo();
        Set<String> allBrokerNameSet = topicPublishInfo.getMessageQueueList().stream().map(MessageQueue::getBrokerName).collect(Collectors.toSet());
        System.out.println(String.format("addNewBrokerAndCheck before allBrokerNameSet %s", allBrokerNameSet));
        assertFalse(allBrokerNameSet.contains(brokerName));

        int newQueueNum = RandomUtil.getIntegerBetween(1, 100);
        MQAdminTestUtils.createTopic(nsAddr, clusterName, topic, newQueueNum, Maps.newHashMap(), 30 * 1000);
        // wait notify client
        Thread.sleep(3000);

        topicPublishInfo = queryTopicPublishInfo();
        assertThat(topicPublishInfo).isNotNull();
        allBrokerNameSet = topicPublishInfo.getMessageQueueList().stream().map(MessageQueue::getBrokerName).collect(Collectors.toSet());
        assertTrue(allBrokerNameSet.contains(brokerName));
        System.out.println(String.format("addNewBrokerAndCheck after allBrokerNameSet %s", allBrokerNameSet));
    }


    private void shutdownBrokerAndCheck(BrokerController brokerController) throws Exception {
        String brokerName = brokerController.getBrokerConfig().getBrokerName();
        TopicPublishInfo topicPublishInfo = queryTopicPublishInfo();
        Set<String> allBrokerNameSet = topicPublishInfo.getMessageQueueList().stream().map(MessageQueue::getBrokerName).collect(Collectors.toSet());
        System.out.println(String.format("shutdownBrokerAndCheck before allBrokerNameSet %s", allBrokerNameSet));
        assertTrue(allBrokerNameSet.contains(brokerName));

        // update topic route info
        brokerController.shutdown();
        // wait notify client
        Thread.sleep(3000);

        topicPublishInfo = queryTopicPublishInfo();
        assertThat(topicPublishInfo).isNotNull();
        allBrokerNameSet = topicPublishInfo.getMessageQueueList().stream().map(MessageQueue::getBrokerName).collect(Collectors.toSet());
        assertFalse(allBrokerNameSet.contains(brokerName));
        System.out.println(String.format("shutdownBrokerAndCheck after allBrokerNameSet %s", allBrokerNameSet));
    }

    /**
     * 1. update topic queue num
     * 2. then sleep 2s
     * 3. check client topic queue num changed
     *
     * @param newQueueNum   the update queue num
     */
    private void modifyQueueNumAndCheck(int newQueueNum) throws Exception {
        // update topic route info
        MQAdminTestUtils.createTopic(nsAddr, clusterName, topic, newQueueNum, Maps.newHashMap(), 30 * 1000);
        // wait notify client
        Thread.sleep(3000);

        TopicPublishInfo topicPublishInfo = queryTopicPublishInfo();
        assertThat(topicPublishInfo).isNotNull();
        List<QueueData> queueDataList = topicPublishInfo.getTopicRouteData().getQueueDatas();
        for (QueueData queueData : queueDataList) {
            assertThat(queueData.getWriteQueueNums()).isEqualTo(newQueueNum);
            assertThat(queueData.getReadQueueNums()).isEqualTo(newQueueNum);
        }
    }

    private TopicPublishInfo queryTopicPublishInfo() throws Exception {
        DefaultMQProducer defaultMQProducer = (DefaultMQProducer) reflectGetAtr(producer, "producer");
        DefaultMQProducerImpl defaultMQProducer2 = (DefaultMQProducerImpl) reflectGetAtr(defaultMQProducer, "defaultMQProducerImpl");
        ConcurrentMap<String, TopicPublishInfo> map = (ConcurrentMap) reflectGetAtr(defaultMQProducer2, "topicPublishInfoTable");
        return map.get(topic);
    }

    private Object reflectGetAtr(Object obj, String fieldName) throws Exception {
        return FieldUtils.readDeclaredField(obj, fieldName, true);
    }

    private boolean isCurrentSystemBusy() throws Exception {
        TopicRouteNotifier topicRouteNotifier = new TopicRouteNotifier(null, null);
        Object systemBusy = MethodUtils.invokeMethod(topicRouteNotifier, true, "isSystemBusy");
        return Boolean.parseBoolean(systemBusy.toString());
    }
}
