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


import org.apache.log4j.Logger;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.namesrv.routeinfo.TopicRouteNotifier;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.truth.Truth.assertThat;

public class TopicRouteUpdateTest extends BaseConf {
    private static final Logger logger = Logger.getLogger(TopicRouteUpdateTest.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        System.out.println("setUp1");
        topic = initTopic();
        System.out.println("setUp2");
        logger.info(String.format("topic is %s", topic));
        producer = getProducer(nsAddr, topic);
        System.out.println("setUp3");
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void testQueryMsg() throws Exception {
        System.out.println("testQueryMsg");
        producer.send(20);

        modifyQueueNumAndCheck(18);
        modifyQueueNumAndCheck(19);
        modifyQueueNumAndCheck(20);
        modifyQueueNumAndCheck(21);
    }

    private void modifyQueueNumAndCheck(int newQueueNum) throws Exception {
        System.out.println(1);
        Field busyFlagField = TopicRouteNotifier.class.getDeclaredField("SYSTEM_BUSY_FLAG");
        busyFlagField.setAccessible(true);
        busyFlagField.set(null, false);
        Field cacheTimeField = TopicRouteNotifier.class.getDeclaredField("LAST_CACHE_TIME");
        cacheTimeField.setAccessible(true);
        cacheTimeField.set(null, System.currentTimeMillis());

        System.out.println(2);


        // update topic route info
        TopicConfig config = new TopicConfig();
        config.setTopicName(topic);
        config.setReadQueueNums(newQueueNum);
        config.setWriteQueueNums(newQueueNum);
        System.out.println(21);
        DefaultMQAdminExt admin = getAdmin(nsAddr);
        System.out.println(22);
        admin.createAndUpdateTopicConfig(brokerController1.getBrokerAddr(), config);

        System.out.println(3);


        // wait notify client
        Thread.sleep(2000);

        TopicPublishInfo topicPublishInfo = queryTopicPublishInfo();
        assertThat(topicPublishInfo).isNotNull();
        List<QueueData> queueDataList = topicPublishInfo.getTopicRouteData().getQueueDatas();
        for (QueueData queueData : queueDataList) {
            assertThat(queueData.getWriteQueueNums()).isEqualTo(newQueueNum);
            assertThat(queueData.getReadQueueNums()).isEqualTo(newQueueNum);
        }
        System.out.println(4);

    }

    private TopicPublishInfo queryTopicPublishInfo() throws Exception {
        DefaultMQProducer defaultMQProducer = (DefaultMQProducer) reflectGetAtr(producer, "producer");
        DefaultMQProducerImpl defaultMQProducer2 = (DefaultMQProducerImpl) reflectGetAtr(defaultMQProducer, "defaultMQProducerImpl");
        ConcurrentMap<String, TopicPublishInfo> map = (ConcurrentMap) reflectGetAtr(defaultMQProducer2, "topicPublishInfoTable");
        return map.get(topic);
    }

    private Object reflectGetAtr(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }

}
