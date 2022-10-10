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
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.namesrv.routeinfo.TopicRouteNotifier;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.truth.Truth.assertThat;

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
        resetProducerAndTopic();
        // wait for the ready
        Thread.sleep(8000);

        producer.send(20);
        for (int queueNum = 100; queueNum < 105; queueNum++) {
            modifyQueueNumAndCheck(queueNum);
        }
//        shutDownProducerAndCheck();
    }

    private boolean isCurrentSystemBusy() throws Exception {
        TopicRouteNotifier topicRouteNotifier = new TopicRouteNotifier(null, null);
        Object systemBusy = MethodUtils.invokeMethod(topicRouteNotifier, true, "isSystemBusy");
        return Boolean.parseBoolean(systemBusy.toString());
    }

    private void shutDownProducerAndCheck() throws Exception {
        resetProducerAndTopic();
        producer.send(20);
        Thread.sleep(2000);
        brokerController1.shutdown();
        RouteInfoManager routeInfoManager = namesrvController.getRouteInfoManager();
        Set<String> changedTopicSet = (Set<String>) reflectGetAtr(routeInfoManager, "changedTopicSet");
        Assert.assertEquals(0, changedTopicSet.size());
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
}
