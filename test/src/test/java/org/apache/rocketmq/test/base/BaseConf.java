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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.base;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.test.client.rmq.RMQAsyncSendProducer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.clientinterface.AbstractMQConsumer;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.AbstractListener;
import org.apache.rocketmq.test.util.MQAdmin;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.test.util.TestUtils;
import org.junit.Assert;

public class BaseConf {
    protected static String nsAddr;
    protected static String broker1Name;
    protected static String broker2Name;
    protected static String clusterName;
    protected static int brokerNum;
    protected static int waitTime = 5;
    protected static int consumeTime = 1 * 60 * 1000;
    protected static int topicCreateTime = 30 * 1000;
    protected static NamesrvController namesrvController;
    protected static BrokerController brokerController1;
    protected static BrokerController brokerController2;
    protected static List<Object> mqClients = new ArrayList<Object>();
    protected static boolean debug = false;
    private static Logger log = Logger.getLogger(BaseConf.class);

    static {
        namesrvController = IntegrationTestBase.createAndStartNamesrv();
        nsAddr = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();
        brokerController1 = IntegrationTestBase.createAndStartBroker(nsAddr);
        brokerController2 = IntegrationTestBase.createAndStartBroker(nsAddr);
        clusterName = brokerController1.getBrokerConfig().getBrokerClusterName();
        broker1Name = brokerController1.getBrokerConfig().getBrokerName();
        broker2Name = brokerController2.getBrokerConfig().getBrokerName();
        brokerNum = 2;
    }

    public BaseConf() {

    }

    public static String initTopic() {
        long startTime = System.currentTimeMillis();
        String topic = MQRandomUtils.getRandomTopic();
        boolean createResult = false;
        while (true) {
            createResult = MQAdmin.createTopic(nsAddr, clusterName, topic, 8);
            if (createResult) {
                break;
            } else if (System.currentTimeMillis() - startTime > topicCreateTime) {
                Assert.fail(String.format("topic[%s] is created failed after:%d ms", topic,
                    System.currentTimeMillis() - startTime));
                break;
            } else {
                TestUtils.waitForMonment(500);
                continue;
            }
        }

        return topic;
    }

    public static String initConsumerGroup() {
        String group = MQRandomUtils.getRandomConsumerGroup();
        return initConsumerGroup(group);
    }

    public static String initConsumerGroup(String group) {
        MQAdmin.createSub(nsAddr, clusterName, group);
        return group;
    }

    public static RMQNormalProducer getProducer(String nsAddr, String topic) {
        RMQNormalProducer producer = new RMQNormalProducer(nsAddr, topic);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQNormalProducer getProducer(String nsAddr, String topic, String producerGoup,
        String instanceName) {
        RMQNormalProducer producer = new RMQNormalProducer(nsAddr, topic, producerGoup,
            instanceName);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQAsyncSendProducer getAsyncProducer(String nsAddr, String topic) {
        RMQAsyncSendProducer producer = new RMQAsyncSendProducer(nsAddr, topic);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String topic, String subExpression,
        AbstractListener listner) {
        String consumerGroup = initConsumerGroup();
        return getConsumer(nsAddr, consumerGroup, topic, subExpression, listner);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String consumerGroup, String topic,
        String subExpression, AbstractListener listner) {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(nsAddr, consumerGroup,
            topic, subExpression, listner);
        if (debug) {
            consumer.setDebug();
        }
        mqClients.add(consumer);
        log.info(String.format("consumer[%s] start,topic[%s],subExpression[%s]", consumerGroup,
            topic, subExpression));
        return consumer;
    }

    public static void shutDown() {
        try {
            for (Object mqClient : mqClients) {
                if (mqClient instanceof AbstractMQProducer) {
                    ((AbstractMQProducer) mqClient).shutdown();

                } else {
                    ((AbstractMQConsumer) mqClient).shutdown();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
