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
import org.apache.log4j.Logger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.sendresult.ResultWrapper;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * test topic not exist case
 */
public class TopicNotExistTest {
    private static final Logger logger = Logger.getLogger(TopicNotExistTest.class);

    private static String nsAddr;
    private static String clusterName;
    private static List<BrokerController> brokerControllers = new ArrayList<>();

    @Test
    public void test() throws Exception {
        clusterStart();

        String topic = "topic_not_exist";
        int pollNameServerInterval = 8000;
        RMQNormalProducer producer = new RMQNormalProducer(nsAddr, topic, false, pollNameServerInterval);
        DefaultMQProducerImpl defaultMQProducerImpl = producer.getProducer().getDefaultMQProducerImpl();
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = defaultMQProducerImpl.getTopicPublishInfoTable();

        assertFalse(topicPublishInfoTable.containsKey(topic));

        // multi send msg
        for (int i = 0; i < 100; i++) {
            producer.send(1);
            ResultWrapper resultWrapper = queryResultWrapper(producer);
            assertFalse(resultWrapper.isSendResult());
            assertTrue(topicPublishInfoTable.containsKey(topic));
            assertFalse(topicPublishInfoTable.get(topic).isTopicExistFlag());
        }

        // create topic
        MQAdminTestUtils.createTopic(nsAddr, clusterName, topic, 8, Maps.newHashMap(), 30 * 1000);
        // wait latest config
        Thread.sleep(pollNameServerInterval + 1000);

        assertTrue(topicPublishInfoTable.get(topic).isTopicExistFlag());
        assertTrue(topicPublishInfoTable.get(topic).isHaveTopicRouterInfo());
        producer.send(1);
        ResultWrapper resultWrapper = queryResultWrapper(producer);
        assertTrue(resultWrapper.isSendResult());
    }

    private ResultWrapper queryResultWrapper(RMQNormalProducer producer) throws IllegalAccessException {
        return (ResultWrapper) FieldUtils.readField(producer, "sendResult", true);
    }

    private void clusterStart() {
        clusterName = "test_test_cluster_name";
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        NamesrvController namesrvController = IntegrationTestBase.createAndStartNamesrv();
        nsAddr = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();

        BrokerController brokerController1 = IntegrationTestBase.createAndStartBroker(nsAddr, clusterName, false);
        BrokerController brokerController2 = IntegrationTestBase.createAndStartBroker(nsAddr, clusterName,false);
        BrokerController brokerController3 = IntegrationTestBase.createAndStartBroker(nsAddr, clusterName,false);

        brokerControllers.add(brokerController1);
        brokerControllers.add(brokerController2);
        brokerControllers.add(brokerController3);

        waitBrokerRegistered();
    }


    private void waitBrokerRegistered() {
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
                return brokerDatas.size() == brokerControllers.size();
            });
            for (BrokerController brokerController: brokerControllers) {
                brokerController.getBrokerOuterAPI().refreshMetadata();
            }
        } catch (Exception e) {
            logger.error("init failed, please check BaseConf", e);
            Assert.fail(e.getMessage());
        }
        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
    }
}
