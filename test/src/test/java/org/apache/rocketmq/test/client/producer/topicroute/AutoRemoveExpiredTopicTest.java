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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

/**
 * test for auto delete expired topic
 */
public class AutoRemoveExpiredTopicTest extends BaseConf {
    private static final Logger logger = Logger.getLogger(AutoRemoveExpiredTopicTest.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void before() {
        topic = initTopic();
        logger.info(String.format("topic is %s", topic));
        producer = getProducer(nsAddr, topic, 200);
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void modifyTopicQueueNumAndCheck() throws Exception {
        long originTopicRouteExpireTime = ClientConfig.getTopicRouteExpireTime();
        try {
            producer.send(1);
            modifyTopicRouteExpireTime(30000);
            Thread.sleep(2000);
            boolean exist = isTopicMetaInProducer(topic);
            Assert.assertTrue(exist);

            producer.send(1);
            modifyTopicRouteExpireTime(1000);
            Thread.sleep(2000);
            exist = isTopicMetaInProducer(topic);
            Assert.assertFalse(exist);

            producer.send(1);
            exist = isTopicMetaInProducer(topic);
            Assert.assertTrue(exist);
        } finally {
            // reset
            modifyTopicRouteExpireTime(originTopicRouteExpireTime);
        }
    }

    private boolean isTopicMetaInProducer(String topic) {
        DefaultMQProducer defaultMQProducer = this.producer.getProducer();
        DefaultMQProducerImpl defaultMQProducerImpl = defaultMQProducer.getDefaultMQProducerImpl();
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = defaultMQProducerImpl.getTopicPublishInfoTable();
        return topicPublishInfoTable.containsKey(topic);
    }


    private void modifyTopicRouteExpireTime(long topicRouteExpireTime) throws Exception {
        FieldUtils.writeStaticField(ClientConfig.class, "topicRouteExpireTime", topicRouteExpireTime, true);

        long actualTime = ClientConfig.getTopicRouteExpireTime();
        Assert.assertEquals(topicRouteExpireTime, actualTime);
    }
}
