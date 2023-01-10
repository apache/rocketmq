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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.test.client.rmq.RMQAsyncSendProducer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQTransactionalProducer;
import org.apache.rocketmq.test.clientinterface.AbstractMQConsumer;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.clientinterface.MQConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.AbstractListener;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.junit.Assert;

import static org.apache.rocketmq.test.base.IntegrationTestBase.initMQAdmin;
import static org.awaitility.Awaitility.await;

public class BaseConf {

    private final static Logger log = LoggerFactory.getLogger(BaseConf.class);

    public final static String NAMESRV_ADDR;

    //the logic queue test need at least three brokers
    protected final static String CLUSTER_NAME;
    protected final static String BROKER1_NAME;
    protected final static String BROKER2_NAME;
    protected final static String BROKER3_NAME;

    protected final static int BROKER_NUM = 3;
    protected final static int WAIT_TIME = 5;
    protected final static int CONSUME_TIME = 2 * 60 * 1000;
    protected final static int QUEUE_NUMBERS = 8;

    protected static NamesrvController namesrvController;
    protected static BrokerController brokerController1;
    protected static BrokerController brokerController2;
    protected static BrokerController brokerController3;
    protected static List<BrokerController> brokerControllerList;
    protected static Map<String, BrokerController> brokerControllerMap;

    protected static List<Object> mqClients = new ArrayList<Object>();
    protected static boolean debug = false;

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        namesrvController = IntegrationTestBase.createAndStartNamesrv();
        NAMESRV_ADDR = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();
        log.debug("Name server started, listening: {}", NAMESRV_ADDR);

        brokerController1 = IntegrationTestBase.createAndStartBroker(NAMESRV_ADDR);
        log.debug("Broker {} started, listening: {}", brokerController1.getBrokerConfig().getBrokerName(),
            brokerController1.getBrokerConfig().getListenPort());

        brokerController2 = IntegrationTestBase.createAndStartBroker(NAMESRV_ADDR);
        log.debug("Broker {} started, listening: {}", brokerController2.getBrokerConfig().getBrokerName(),
            brokerController2.getBrokerConfig().getListenPort());

        brokerController3 = IntegrationTestBase.createAndStartBroker(NAMESRV_ADDR);
        log.debug("Broker {} started, listening: {}", brokerController2.getBrokerConfig().getBrokerName(),
            brokerController2.getBrokerConfig().getListenPort());

        CLUSTER_NAME = brokerController1.getBrokerConfig().getBrokerClusterName();
        BROKER1_NAME = brokerController1.getBrokerConfig().getBrokerName();
        BROKER2_NAME = brokerController2.getBrokerConfig().getBrokerName();
        BROKER3_NAME = brokerController3.getBrokerConfig().getBrokerName();
        brokerControllerList = ImmutableList.of(brokerController1, brokerController2, brokerController3);
        brokerControllerMap = brokerControllerList.stream().collect(
            Collectors.toMap(input -> input.getBrokerConfig().getBrokerName(), Function.identity()));
        initMQAdmin(NAMESRV_ADDR);
    }

    public BaseConf() {
        // Add waitBrokerRegistered to BaseConf constructor to make it default for all subclasses.
        waitBrokerRegistered(NAMESRV_ADDR, CLUSTER_NAME, BROKER_NUM);
    }

    // This method can't be placed in the static block of BaseConf, which seems to lead to a strange dead lock.
    public static void waitBrokerRegistered(final String nsAddr, final String clusterName, final int expectedBrokerNum) {
        final DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(500);
        mqAdminExt.setNamesrvAddr(nsAddr);
        try {
            mqAdminExt.start();
            await().atMost(30, TimeUnit.SECONDS).until(() -> {
                List<BrokerData> brokerDatas;
                try {
                    brokerDatas = mqAdminExt.examineTopicRouteInfo(clusterName).getBrokerDatas();
                } catch (Exception e) {
                    return false;
                }
                return brokerDatas.size() == expectedBrokerNum;
            });
            for (BrokerController brokerController: brokerControllerList) {
                brokerController.getBrokerOuterAPI().refreshMetadata();
            }
        } catch (Exception e) {
            log.error("init failed, please check BaseConf", e);
            Assert.fail(e.getMessage());
        }
        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
    }

    public boolean awaitDispatchMs(long timeMs) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= timeMs) {
            boolean allOk = true;
            for (BrokerController brokerController: brokerControllerList) {
                if (brokerController.getMessageStore().dispatchBehindBytes() != 0) {
                    allOk = false;
                    break;
                }
            }
            if (allOk) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }


    public static String initTopic() {
        String topic = MQRandomUtils.getRandomTopic();
        return initTopicWithName(topic);
    }

    public static String initTopic(TopicMessageType topicMessageType) {
        String topic = MQRandomUtils.getRandomTopic();
        return initTopicWithName(topic, topicMessageType);
    }

    public static String initTopicOnSampleTopicBroker(String sampleTopic) {
        String topic = MQRandomUtils.getRandomTopic();
        return initTopicOnSampleTopicBroker(topic, sampleTopic);
    }

    public static String initTopicOnSampleTopicBroker(String sampleTopic, TopicMessageType topicMessageType) {
        String topic = MQRandomUtils.getRandomTopic();
        return initTopicOnSampleTopicBroker(topic, sampleTopic, topicMessageType);
    }

    public static String initTopicWithName(String topicName) {
        IntegrationTestBase.initTopic(topicName, NAMESRV_ADDR, CLUSTER_NAME, CQType.SimpleCQ);
        return topicName;
    }

    public static String initTopicWithName(String topicName, TopicMessageType topicMessageType) {
        IntegrationTestBase.initTopic(topicName, NAMESRV_ADDR, CLUSTER_NAME, topicMessageType);
        return topicName;
    }

    public static String initTopicOnSampleTopicBroker(String topicName, String sampleTopic) {
        IntegrationTestBase.initTopic(topicName, NAMESRV_ADDR, sampleTopic, CQType.SimpleCQ);
        return topicName;
    }

    public static String initTopicOnSampleTopicBroker(String topicName, String sampleTopic, TopicMessageType topicMessageType) {
        IntegrationTestBase.initTopic(topicName, NAMESRV_ADDR, sampleTopic, topicMessageType);
        return topicName;
    }

    public static String initConsumerGroup() {
        String group = MQRandomUtils.getRandomConsumerGroup();
        return initConsumerGroup(group);
    }

    public static String initConsumerGroup(String group) {
        MQAdminTestUtils.createSub(NAMESRV_ADDR, CLUSTER_NAME, group);
        return group;
    }

    public static DefaultMQAdminExt getAdmin(String nsAddr) {
        final DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(3 * 1000);
        mqAdminExt.setNamesrvAddr(nsAddr);
        mqAdminExt.setPollNameServerInterval(100);
        mqClients.add(mqAdminExt);
        return mqAdminExt;
    }


    public static RMQNormalProducer getProducer(String nsAddr, String topic) {
        return getProducer(nsAddr, topic, false);
    }

    public static RMQNormalProducer getProducer(String nsAddr, String topic, boolean useTLS) {
        RMQNormalProducer producer = new RMQNormalProducer(nsAddr, topic, useTLS);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQTransactionalProducer getTransactionalProducer(String nsAddr, String topic, TransactionListener transactionListener) {
        RMQTransactionalProducer producer = new RMQTransactionalProducer(nsAddr, topic, false, transactionListener);
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
                                                AbstractListener listener) {
        return getConsumer(nsAddr, topic, subExpression, listener, false);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String topic, String subExpression,
                                                AbstractListener listener, boolean useTLS) {
        String consumerGroup = initConsumerGroup();
        return getConsumer(nsAddr, consumerGroup, topic, subExpression, listener, useTLS);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String consumerGroup, String topic,
                                                String subExpression, AbstractListener listener) {
        return getConsumer(nsAddr, consumerGroup, topic, subExpression, listener, false);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String consumerGroup, String topic,
                                                String subExpression, AbstractListener listener, boolean useTLS) {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(nsAddr, consumerGroup,
                topic, subExpression, listener, useTLS);
        if (debug) {
            consumer.setDebug();
        }
        mqClients.add(consumer);
        log.info(String.format("consumer[%s] start,topic[%s],subExpression[%s]", consumerGroup,
                topic, subExpression));
        return consumer;
    }

    public static void shutdown() {
        ImmutableList<Object> mqClients = ImmutableList.copyOf(BaseConf.mqClients);
        BaseConf.mqClients.clear();
        shutdown(mqClients);
    }

    public static Set<String> getBrokers() {
        Set<String> brokers = new HashSet<>();
        brokers.add(BROKER1_NAME);
        brokers.add(BROKER2_NAME);
        brokers.add(BROKER3_NAME);
        return brokers;
    }

    public static void shutdown(List<Object> mqClients) {
        mqClients.forEach(mqClient -> ForkJoinPool.commonPool().execute(() -> {
            if (mqClient instanceof AbstractMQProducer) {
                ((AbstractMQProducer) mqClient).shutdown();
            } else if (mqClient instanceof AbstractMQConsumer) {
                ((AbstractMQConsumer) mqClient).shutdown();
            } else if (mqClient instanceof MQAdminExt) {
                ((MQAdminExt) mqClient).shutdown();
            } else if (mqClient instanceof MQProducer) {
                ((MQProducer) mqClient).shutdown();
            } else if (mqClient instanceof MQPullConsumer) {
                ((MQPullConsumer) mqClient).shutdown();
            } else if (mqClient instanceof MQPushConsumer) {
                ((MQPushConsumer) mqClient).shutdown();
            } else if (mqClient instanceof MQConsumer) {
                ((MQConsumer) mqClient).shutdown();
            }
        }));
    }
}
