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

package org.apache.rocketmq.test.retry;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.offset.OffsetResetIT;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class PopConsumerRetryIT extends BaseConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetIT.class);

    private DefaultMQAdminExt defaultMQAdminExt = null;
    private String topicName = null;
    private String groupName = null;

    @Before
    public void init() throws MQClientException {
        topicName = "topic-" + RandomStringUtils.randomAlphabetic(72).toUpperCase();
        groupName = "group-" + RandomStringUtils.randomAlphabetic(72).toUpperCase();
        LOGGER.info(String.format("use topic: %s, group: %s", topicName, groupName));
        IntegrationTestBase.initTopic(topicName, NAMESRV_ADDR, CLUSTER_NAME, CQType.SimpleCQ);
        defaultMQAdminExt = getAdmin(NAMESRV_ADDR);
        defaultMQAdminExt.start();
    }

    @After
    public void tearDown() {
        shutdown();
    }

    private void switchPop(String groupName, String topicName) throws Exception {
        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        Set<String> brokerAddrs = clusterInfo.getBrokerAddrTable().values()
            .stream().map(BrokerData::selectBrokerAddr).collect(Collectors.toSet());
        for (String brokerAddr : brokerAddrs) {
            TopicConfig topicConfig = new TopicConfig(topicName, 1, 1, 6);
            defaultMQAdminExt.createAndUpdateTopicConfig(brokerAddr, topicConfig);
            defaultMQAdminExt.setMessageRequestMode(brokerAddr, topicName, groupName,
                MessageRequestMode.POP, 8, 3000L);
        }
    }

    @Test
    public void testNormalMessageUseMessageVersionV2() throws Exception {
        switchPop(groupName, topicName);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger retryCount = new AtomicInteger();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.subscribe(topicName, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt message : msgs) {
                LOGGER.debug(String.format("messageId: %s, times: %d, topic: %s",
                    message.getMsgId(), message.getReconsumeTimes(), message.getTopic()));
                if (message.getReconsumeTimes() < 2) {
                    retryCount.incrementAndGet();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                } else {
                    successCount.incrementAndGet();
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        LOGGER.info("Consumer Started...");

        DefaultMQProducer producer = new DefaultMQProducer("PID-1", false, null);
        producer.setAccessChannel(AccessChannel.CLOUD);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        LOGGER.info("Producer Started...%n");

        // wait pop client register
        TimeUnit.SECONDS.sleep(3);

        int total = 10;
        for (int i = 0; i < total; i++) {
            Message msg = new Message(
                topicName, "*", "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        }

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
            .until(() -> {
                LOGGER.debug(String.format("retry: %d, success: %d", retryCount.get(), successCount.get()));
                return retryCount.get() == total * 2 && successCount.get() == total;
            });
    }

    @Test
    public void testFIFOMessageUseMessageVersionV2() throws Exception {
        switchPop(groupName, topicName);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger retryCount = new AtomicInteger();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.subscribe(topicName, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            for (MessageExt message : msgs) {
                LOGGER.debug(String.format("messageId: %s, times: %d, topic: %s",
                    message.getMsgId(), message.getReconsumeTimes(), message.getTopic()));
                if (message.getReconsumeTimes() < 2) {
                    retryCount.incrementAndGet();
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                } else {
                    successCount.incrementAndGet();
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
        LOGGER.info("Consumer Started...");

        DefaultMQProducer producer = new DefaultMQProducer("PID-1", false, null);
        producer.setAccessChannel(AccessChannel.CLOUD);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        LOGGER.info("Producer Started...%n");

        // wait pop client register
        TimeUnit.SECONDS.sleep(3);

        int total = 10;
        for (int i = 0; i < total; i++) {
            Message msg = new Message(
                topicName, "*", "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        }

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
            .until(() -> {
                LOGGER.debug(String.format("retry: %d, success: %d", retryCount.get(), successCount.get()));
                return retryCount.get() == total * 2 && successCount.get() == total;
            });
    }
}
