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
package org.apache.rocketmq.test.client.consumer.balance;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class RebalanceClientIT extends BaseConf {

    String pullTopic = "pull_topic";
    String popTopic = "pop_topic";
    String group = "group";
    private DefaultMQAdminExt defaultMQAdminExt = null;
    Set<MessageQueue> pullQueue;
    Set<MessageQueue> pullQueue1;
    Set<MessageQueue> pullQueue2;
    Set<MessageQueue> pullQueue3;
    Set<MessageQueue> pullQueue4;
    Set<MessageQueue> pullQueue5;
    Set<MessageQueue> popQueue;
    Set<MessageQueue> popQueue1;
    Set<MessageQueue> popQueue2;
    Set<MessageQueue> popQueue3;
    Set<MessageQueue> popQueue4;
    Set<MessageQueue> popQueue5;

    @Before
    public void init() throws MQClientException {
        IntegrationTestBase.initTopic(popTopic, NAMESRV_ADDR, CLUSTER_NAME, CQType.SimpleCQ);
        IntegrationTestBase.initTopic(pullTopic, NAMESRV_ADDR, CLUSTER_NAME, CQType.SimpleCQ);
        defaultMQAdminExt = getAdmin(NAMESRV_ADDR);
        defaultMQAdminExt.start();
    }

    @Test
    public void testRebalanceLogic1() throws Exception {
        testRebalanceLogicInBroker();
        testRebalanceLogicInClient();
        Assert.assertEquals(pullQueue, pullQueue2);
        Assert.assertEquals(pullQueue1, pullQueue3);
        Assert.assertEquals(popQueue, popQueue2);
        Assert.assertEquals(popQueue1, popQueue3);
    }

    private void testRebalanceLogicInBroker() throws Exception {
        switchPop(group, popTopic, null);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.subscribe(popTopic, "*");
        consumer.subscribe(pullTopic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer.start();
        consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().doRebalance(false);

        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(group);
        consumer1.subscribe(popTopic, "*");
        consumer1.subscribe(pullTopic, "*");
        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer1.setConsumeThreadMin(1);
        consumer1.setConsumeThreadMax(1);
        consumer1.setConsumeMessageBatchMaxSize(1);
        consumer1.setNamesrvAddr(NAMESRV_ADDR);
        consumer1.setMessageModel(MessageModel.CLUSTERING);
        consumer1.setClientRebalance(false);
        consumer1.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer1.start();
        consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().doRebalance(false);
        consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().doRebalance(false);

        Thread.sleep(3000);
        // old logic
        consumer1.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();
        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();

        pullQueue = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());
        pullQueue1 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue1 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());

        Assert.assertEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertEquals(0, consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPullTopicRebalance().size());
        Assert.assertEquals(0, consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPullTopicRebalance().size());

        // ensure rebalance in broker
        Assert.assertNotEquals(0, pullQueue.size());
        Assert.assertNotEquals(0, popQueue.size());
        Assert.assertNotEquals(0, pullQueue1.size());
        Assert.assertNotEquals(0, popQueue1.size());

        consumer1.shutdown();
        consumer.shutdown();
        Thread.sleep(3000);
    }

    private void testRebalanceLogicInClient() throws Exception {
        switchPop(group, popTopic, null);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.subscribe(popTopic, "*");
        consumer.subscribe(pullTopic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setEnableRebalanceTransferInPop(true);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer.start();
        consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().doRebalance(false);

        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(group);
        consumer1.subscribe(popTopic, "*");
        consumer1.subscribe(pullTopic, "*");
        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer1.setConsumeThreadMin(1);
        consumer1.setConsumeThreadMax(1);
        consumer1.setConsumeMessageBatchMaxSize(1);
        consumer1.setNamesrvAddr(NAMESRV_ADDR);
        consumer1.setMessageModel(MessageModel.CLUSTERING);
        consumer1.setClientRebalance(false);
        consumer1.setEnableRebalanceTransferInPop(true);
        consumer1.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer1.start();

        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getClientConfig().setEnableRebalanceTransferInPop(true);
        consumer1.getDefaultMQPushConsumerImpl().getmQClientFactory().getClientConfig().setEnableRebalanceTransferInPop(true);
        Thread.sleep(3000);

        // get pop topic
        ((RebalancePushImpl) consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl()).updateMOdeCache();
        ((RebalancePushImpl) consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl()).updateMOdeCache();

        Assert.assertNotEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertNotEquals(0, consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());

        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();
        consumer1.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();

        pullQueue2 = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue2 = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());
        pullQueue3 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue3 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());

        Assert.assertNotEquals(0, pullQueue2.size());
        Assert.assertNotEquals(0, popQueue2.size());
        Assert.assertNotEquals(0, pullQueue3.size());
        Assert.assertNotEquals(0, popQueue3.size());

        consumer1.shutdown();
        consumer.shutdown();
        Thread.sleep(3000);
    }

    private void switchPop(String groupName, String topicName, Integer popShareQueueNum) throws Exception {
        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        Set<String> brokerAddrs = clusterInfo.getBrokerAddrTable().values()
                .stream().map(BrokerData::selectBrokerAddr).collect(Collectors.toSet());
        for (String brokerAddr : brokerAddrs) {
            TopicConfig topicConfig = new TopicConfig(topicName, 1, 1, 6);
            defaultMQAdminExt.createAndUpdateTopicConfig(brokerAddr, topicConfig);
            defaultMQAdminExt.setMessageRequestMode(brokerAddr, topicName, groupName,
                    MessageRequestMode.POP, popShareQueueNum != null ? popShareQueueNum : 8, 3000L);
        }
    }

    @Test
    public void testBrokerNotifyClient() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.subscribe(popTopic, "*");
        consumer.subscribe(pullTopic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setEnableRebalanceTransferInPop(true);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer.start();
        Thread.sleep(1000);
        Assert.assertEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());

        switchPop(popTopic, group, null);
        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().updateRebalanceByBrokerAndClientMap(group, popTopic, MessageRequestMode.POP, 8);
        Assert.assertTrue(consumer.isClientRebalance());
        Assert.assertNotEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
    }

    @Test
    public void testBrokerNotifyClient2() throws Exception {
        testRebalanceInBroker();
        testRebalanceInClient();
        Assert.assertEquals(pullQueue, pullQueue3);
        Assert.assertEquals(pullQueue1, pullQueue4);
        Assert.assertEquals(pullQueue2, pullQueue5);
        Assert.assertEquals(popQueue, popQueue3);
        Assert.assertEquals(popQueue1, popQueue4);
        Assert.assertEquals(popQueue2, popQueue5);
    }

    private void testRebalanceInClient() throws Exception {
        switchPop(group, popTopic, 1);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.subscribe(popTopic, "*");
        consumer.subscribe(pullTopic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setEnableRebalanceTransferInPop(true);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer.start();
        consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().doRebalance(false);

        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(group);
        consumer1.subscribe(popTopic, "*");
        consumer1.subscribe(pullTopic, "*");
        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer1.setConsumeThreadMin(1);
        consumer1.setConsumeThreadMax(1);
        consumer1.setConsumeMessageBatchMaxSize(1);
        consumer1.setNamesrvAddr(NAMESRV_ADDR);
        consumer1.setMessageModel(MessageModel.CLUSTERING);
        consumer1.setClientRebalance(false);
        consumer1.setEnableRebalanceTransferInPop(true);
        consumer1.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer1.start();

        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(group);
        consumer2.subscribe(popTopic, "*");
        consumer2.subscribe(pullTopic, "*");
        consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer2.setConsumeThreadMin(1);
        consumer2.setConsumeThreadMax(1);
        consumer2.setConsumeMessageBatchMaxSize(1);
        consumer2.setNamesrvAddr(NAMESRV_ADDR);
        consumer2.setMessageModel(MessageModel.CLUSTERING);
        consumer2.setClientRebalance(false);
        consumer2.setEnableRebalanceTransferInPop(true);
        consumer2.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer2.start();

        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getClientConfig().setEnableRebalanceTransferInPop(true);
        consumer1.getDefaultMQPushConsumerImpl().getmQClientFactory().getClientConfig().setEnableRebalanceTransferInPop(true);
        consumer2.getDefaultMQPushConsumerImpl().getmQClientFactory().getClientConfig().setEnableRebalanceTransferInPop(true);
        Thread.sleep(3000);

        // get pop topic
        ((RebalancePushImpl) consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl()).updateMOdeCache();
        ((RebalancePushImpl) consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl()).updateMOdeCache();
        ((RebalancePushImpl) consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl()).updateMOdeCache();

        Assert.assertNotEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertNotEquals(0, consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertNotEquals(0, consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());

        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();
        consumer1.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();
        consumer2.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();

        pullQueue = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());
        pullQueue1 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue1 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());
        pullQueue2 = new HashSet<>(consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue2 = new HashSet<>(consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());

        Assert.assertNotEquals(0, pullQueue.size());
        Assert.assertNotEquals(0, popQueue.size());
        Assert.assertNotEquals(0, pullQueue1.size());
        Assert.assertNotEquals(0, popQueue1.size());
        Assert.assertNotEquals(0, pullQueue2.size());
        Assert.assertNotEquals(0, popQueue2.size());

        consumer1.shutdown();
        consumer.shutdown();
        consumer2.shutdown();
        Thread.sleep(3000);
    }

    private void testRebalanceInBroker() throws Exception {
        switchPop(group, popTopic, 1);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.subscribe(popTopic, "*");
        consumer.subscribe(pullTopic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setEnableRebalanceTransferInPop(true);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setClientRebalance(false);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer.start();
        consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().doRebalance(false);

        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(group);
        consumer1.subscribe(popTopic, "*");
        consumer1.subscribe(pullTopic, "*");
        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer1.setConsumeThreadMin(1);
        consumer1.setConsumeThreadMax(1);
        consumer1.setConsumeMessageBatchMaxSize(1);
        consumer1.setNamesrvAddr(NAMESRV_ADDR);
        consumer1.setMessageModel(MessageModel.CLUSTERING);
        consumer1.setClientRebalance(false);
        consumer1.setEnableRebalanceTransferInPop(true);
        consumer1.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer1.start();

        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(group);
        consumer2.subscribe(popTopic, "*");
        consumer2.subscribe(pullTopic, "*");
        consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer2.setConsumeThreadMin(1);
        consumer2.setConsumeThreadMax(1);
        consumer2.setConsumeMessageBatchMaxSize(1);
        consumer2.setNamesrvAddr(NAMESRV_ADDR);
        consumer2.setMessageModel(MessageModel.CLUSTERING);
        consumer2.setClientRebalance(false);
        consumer2.setEnableRebalanceTransferInPop(true);
        consumer2.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer2.start();

        Thread.sleep(3000);
        // old logic
        consumer1.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();
        consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().doRebalance();

        pullQueue3 = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue3 = new HashSet<>(consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());
        pullQueue4 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue4 = new HashSet<>(consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());
        pullQueue5 = new HashSet<>(consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable().keySet());
        popQueue5 = new HashSet<>(consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopProcessQueueTable().keySet());

        Assert.assertEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertEquals(0, consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertEquals(0, consumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPullTopicRebalance().size());
        Assert.assertEquals(0, consumer1.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPullTopicRebalance().size());
        Assert.assertEquals(0, consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPopTopicRebalance().size());
        Assert.assertEquals(0, consumer2.getDefaultMQPushConsumerImpl().getRebalanceImpl().getPullTopicRebalance().size());

        // ensure rebalance in broker
        Assert.assertNotEquals(0, pullQueue3.size());
        Assert.assertNotEquals(0, popQueue3.size());
        Assert.assertNotEquals(0, pullQueue4.size());
        Assert.assertNotEquals(0, popQueue4.size());
        Assert.assertNotEquals(0, pullQueue5.size());
        Assert.assertNotEquals(0, popQueue5.size());

        consumer1.shutdown();
        consumer.shutdown();
        consumer2.shutdown();
        Thread.sleep(3000);
    }

    @After
    public void tearDown() {
        shutdown();
    }
}
