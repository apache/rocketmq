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

package org.apache.rocketmq.test.statictopic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingOne;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.rpc.ClientMetadata;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils.getMappingDetailFromConfig;

@FixMethodOrder
public class StaticTopicIT extends BaseConf {

    private static Logger logger = LoggerFactory.getLogger(StaticTopicIT.class);
    private DefaultMQAdminExt defaultMQAdminExt;

    @Before
    public void setUp() throws Exception {
        System.setProperty("rocketmq.client.rebalance.waitInterval", "500");
        defaultMQAdminExt = getAdmin(NAMESRV_ADDR);
        waitBrokerRegistered(NAMESRV_ADDR, CLUSTER_NAME, BROKER_NUM);
        defaultMQAdminExt.start();
    }


    @Test
    public void testCommandsWithCluster() throws Exception {
        //This case is used to mock the env to test the command manually
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());
        int queueNum = 10;
        int msgEachQueue = 100;

        {
            MQAdminTestUtils.createStaticTopicWithCommand(topic, queueNum, null, CLUSTER_NAME, NAMESRV_ADDR);
            sendMessagesAndCheck(producer, getBrokers(), topic, queueNum, msgEachQueue, 0);
            //consume and check
            consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 1);
        }
        {
            MQAdminTestUtils.remappingStaticTopicWithCommand(topic, null, CLUSTER_NAME, NAMESRV_ADDR);
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), consumer.getConsumer(), defaultMQAdminExt);
            sendMessagesAndCheck(producer, getBrokers(), topic, queueNum, msgEachQueue, msgEachQueue);
        }
    }

    @Test
    public void testCommandsWithBrokers() throws Exception {
        //This case is used to mock the env to test the command manually
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());
        int queueNum = 10;
        int msgEachQueue = 10;
        {
            Set<String> brokers = ImmutableSet.of(BROKER1_NAME);
            MQAdminTestUtils.createStaticTopicWithCommand(topic, queueNum, brokers, null, NAMESRV_ADDR);
            sendMessagesAndCheck(producer, brokers, topic, queueNum, msgEachQueue, 0);
            //consume and check
            consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 1);
        }
        {
            Set<String> brokers = ImmutableSet.of(BROKER2_NAME);
            MQAdminTestUtils.remappingStaticTopicWithCommand(topic, brokers, null, NAMESRV_ADDR);
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), consumer.getConsumer(), defaultMQAdminExt);
            sendMessagesAndCheck(producer, brokers, topic, queueNum, msgEachQueue, TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE);
            consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 2);
        }
    }

    @Test
    public void testNoTargetBrokers() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        int queueNum = 10;
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(BROKER1_NAME);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(BROKER_NUM, remoteBrokerConfigMap.size());
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
            TopicConfigAndQueueMapping configMapping = remoteBrokerConfigMap.get(BROKER2_NAME);
            Assert.assertEquals(0, configMapping.getWriteQueueNums());
            Assert.assertEquals(0, configMapping.getReadQueueNums());
            Assert.assertEquals(0, configMapping.getMappingDetail().getHostedQueues().size());
        }

        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(BROKER2_NAME);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(BROKER_NUM, remoteBrokerConfigMap.size());
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
        }

    }

    private void sendMessagesAndCheck(RMQNormalProducer producer, Set<String> targetBrokers, String topic, int queueNum, int msgEachQueue, long baseOffset) throws Exception {
        ClientMetadata clientMetadata = MQAdminUtils.getBrokerAndTopicMetadata(topic, defaultMQAdminExt);
        List<MessageQueue> messageQueueList = producer.getMessageQueue();
        Assert.assertEquals(queueNum, messageQueueList.size());
        for (int i = 0; i < queueNum; i++) {
            MessageQueue messageQueue = messageQueueList.get(i);
            Assert.assertEquals(topic, messageQueue.getTopic());
            Assert.assertEquals(TopicQueueMappingUtils.getMockBrokerName(MixAll.METADATA_SCOPE_GLOBAL), messageQueue.getBrokerName());
            Assert.assertEquals(i, messageQueue.getQueueId());
            String destBrokerName = clientMetadata.getBrokerNameFromMessageQueue(messageQueue);
            Assert.assertTrue(targetBrokers.contains(destBrokerName));
        }
        for (MessageQueue messageQueue: messageQueueList) {
            producer.send(msgEachQueue, messageQueue);
        }
        Assert.assertEquals(0, producer.getSendErrorMsg().size());
        //leave the time to build the cq
        Assert.assertTrue(awaitDispatchMs(500));
        for (MessageQueue messageQueue : messageQueueList) {
            Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
            Assert.assertEquals(msgEachQueue + baseOffset, defaultMQAdminExt.maxOffset(messageQueue));
        }
        TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topic);
        for (MessageQueue messageQueue : messageQueueList) {
            Assert.assertEquals(0, topicStatsTable.getOffsetTable().get(messageQueue).getMinOffset());
            Assert.assertEquals(msgEachQueue + baseOffset, topicStatsTable.getOffsetTable().get(messageQueue).getMaxOffset());
        }
    }

    private Map<Integer, List<MessageExt>> computeMessageByQueue(Collection<Object> msgs) {
        Map<Integer, List<MessageExt>> messagesByQueue = new HashMap<>();
        for (Object object : msgs) {
            MessageExt messageExt = (MessageExt) object;
            if (!messagesByQueue.containsKey(messageExt.getQueueId())) {
                messagesByQueue.put(messageExt.getQueueId(), new ArrayList<>());
            }
            messagesByQueue.get(messageExt.getQueueId()).add(messageExt);
        }
        for (List<MessageExt> msgEachQueue : messagesByQueue.values()) {
            msgEachQueue.sort((o1, o2) -> (int) (o1.getQueueOffset() - o2.getQueueOffset()));
        }
        return messagesByQueue;
    }

    private void consumeMessagesAndCheck(RMQNormalProducer producer, RMQNormalConsumer consumer, String topic, int queueNum, int msgEachQueue, int startGen, int genNum) {
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), 60000);

        Assert.assertEquals(producer.getAllMsgBody().size(), consumer.getListener().getAllMsgBody().size());
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer.getListener().getAllMsgBody()))
                .containsExactlyElementsIn(producer.getAllMsgBody());
        Map<Integer, List<MessageExt>> messagesByQueue = computeMessageByQueue(consumer.getListener().getAllOriginMsg());
        Assert.assertEquals(queueNum, messagesByQueue.size());
        for (int i = 0; i < queueNum; i++) {
            List<MessageExt> messageExts = messagesByQueue.get(i);

            int totalEachQueue = msgEachQueue * genNum;
            Assert.assertEquals(totalEachQueue, messageExts.size());
            for (int j = 0; j < totalEachQueue; j++) {
                MessageExt messageExt = messageExts.get(j);
                int currGen = startGen + j / msgEachQueue;
                Assert.assertEquals(topic, messageExt.getTopic());
                Assert.assertEquals(TopicQueueMappingUtils.getMockBrokerName(MixAll.METADATA_SCOPE_GLOBAL), messageExt.getBrokerName());
                Assert.assertEquals(i, messageExt.getQueueId());
                Assert.assertEquals((j % msgEachQueue) + currGen * TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, messageExt.getQueueOffset());
            }
        }
    }


    @Test
    public void testCreateProduceConsumeStaticTopic() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());

        int queueNum = 10;
        int msgEachQueue = 10;
        //create static topic
        Map<String, TopicConfigAndQueueMapping> localBrokerConfigMap = MQAdminTestUtils.createStaticTopic(topic, queueNum, getBrokers(), defaultMQAdminExt);
        //check the static topic config
        {
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(BROKER_NUM, remoteBrokerConfigMap.size());
            for (Map.Entry<String, TopicConfigAndQueueMapping> entry: remoteBrokerConfigMap.entrySet())  {
                String broker = entry.getKey();
                TopicConfigAndQueueMapping configMapping = entry.getValue();
                TopicConfigAndQueueMapping localConfigMapping = localBrokerConfigMap.get(broker);
                Assert.assertNotNull(localConfigMapping);
                Assert.assertEquals(configMapping, localConfigMapping);
            }
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
        }
        //send and check
        sendMessagesAndCheck(producer, getBrokers(), topic, queueNum, msgEachQueue, 0);
        //consume and check
        consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 1);
    }


    @Test
    public void testRemappingProduceConsumeStaticTopic() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());

        int queueNum = 1;
        int msgEachQueue = 10;
        //create send consume
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER1_NAME);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 0);
            consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 1);
        }
        //remapping the static topic
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER2_NAME);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
            for (TopicQueueMappingOne mappingOne: globalIdMap.values()) {
                Assert.assertEquals(BROKER2_NAME, mappingOne.getBname());
                Assert.assertEquals(TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, mappingOne.getItems().get(mappingOne.getItems().size() - 1).getLogicOffset());
            }
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), consumer.getConsumer(), defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE);
            consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 2);
        }
    }


    public boolean awaitRefreshStaticTopicMetadata(long timeMs, String topic, DefaultMQProducer producer, DefaultMQPushConsumer consumer, DefaultMQAdminExt adminExt) throws Exception {
        long start = System.currentTimeMillis();
        MQClientInstance currentInstance = null;
        while (System.currentTimeMillis() - start <= timeMs) {
            boolean allOk = true;
            if (producer != null) {
                currentInstance = producer.getDefaultMQProducerImpl().getmQClientFactory();
                currentInstance.updateTopicRouteInfoFromNameServer(topic);
                if (!MQAdminTestUtils.checkStaticTopic(topic, adminExt, currentInstance)) {
                    allOk = false;
                }
            }
            if (consumer != null) {
                currentInstance = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory();
                currentInstance.updateTopicRouteInfoFromNameServer(topic);
                if (!MQAdminTestUtils.checkStaticTopic(topic, adminExt, currentInstance)) {
                    allOk = false;
                }
            }
            if (adminExt != null) {
                currentInstance = adminExt.getDefaultMQAdminExtImpl().getMqClientInstance();
                currentInstance.updateTopicRouteInfoFromNameServer(topic);
                if (!MQAdminTestUtils.checkStaticTopic(topic, adminExt, currentInstance)) {
                    allOk = false;
                }
            }
            if (allOk) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }


    @Test
    public void testDoubleReadCheckConsumerOffset() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        String group = initConsumerGroup();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, group, topic, "*", new RMQNormalListener());
        long start = System.currentTimeMillis();

        int queueNum = 5;
        int msgEachQueue = 10;
        //create static topic
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER1_NAME);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 0);
            consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 0, 1);
        }
        producer.shutdown();
        consumer.shutdown();
        //use a new producer
        producer = getProducer(NAMESRV_ADDR, topic);

        ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(group);
        List<MessageQueue> messageQueues = producer.getMessageQueue();
        for (MessageQueue queue: messageQueues) {
            OffsetWrapper wrapper = consumeStats.getOffsetTable().get(queue);
            Assert.assertNotNull(wrapper);
            Assert.assertEquals(msgEachQueue, wrapper.getBrokerOffset());
            Assert.assertEquals(msgEachQueue, wrapper.getConsumerOffset());
            Assert.assertTrue(wrapper.getLastTimestamp() > start);
        }

        List<String> brokers = ImmutableList.of(BROKER2_NAME, BROKER3_NAME, BROKER1_NAME);
        for (int i = 0; i < brokers.size(); i++) {
            Set<String> targetBrokers = ImmutableSet.of(brokers.get(i));
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            //make the metadata
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), null, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, (i + 1) * TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE);
        }

        TestUtils.waitForSeconds(1);
        consumeStats = defaultMQAdminExt.examineConsumeStats(group);

        messageQueues = producer.getMessageQueue();
        for (MessageQueue queue: messageQueues) {
            OffsetWrapper wrapper = consumeStats.getOffsetTable().get(queue);
            Assert.assertNotNull(wrapper);
            Assert.assertEquals(msgEachQueue + brokers.size() * TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, wrapper.getBrokerOffset());
            Assert.assertEquals(msgEachQueue, wrapper.getConsumerOffset());
            Assert.assertTrue(wrapper.getLastTimestamp() > start);
        }
        consumer = getConsumer(NAMESRV_ADDR, group, topic, "*", new RMQNormalListener());
        consumeMessagesAndCheck(producer, consumer, topic, queueNum, msgEachQueue, 1, brokers.size());
    }




    @Test
    public void testRemappingAndClear() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        int queueNum = 10;
        int msgEachQueue = 100;
        //create to broker1Name
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER1_NAME);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            //leave the time to refresh the metadata
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), null, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 0);
        }

        //remapping to broker2Name
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER2_NAME);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            //leave the time to refresh the metadata
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), null, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 1 * TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE);
        }

        //remapping to broker3Name
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER3_NAME);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            //leave the time to refresh the metadata
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), null, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 2 * TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE);
        }

        // 1 -> 2 -> 3, currently 1 should not have any mappings

        {
            for (int i = 0; i < 10; i++) {
                for (BrokerController brokerController: brokerControllerList) {
                    brokerController.getTopicQueueMappingCleanService().wakeup();
                }
                Thread.sleep(100);
            }
            Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(BROKER_NUM, brokerConfigMap.size());
            TopicConfigAndQueueMapping config1 = brokerConfigMap.get(BROKER1_NAME);
            TopicConfigAndQueueMapping config2 = brokerConfigMap.get(BROKER2_NAME);
            TopicConfigAndQueueMapping config3 = brokerConfigMap.get(BROKER3_NAME);
            Assert.assertEquals(0, config1.getMappingDetail().getHostedQueues().size());
            Assert.assertEquals(queueNum, config2.getMappingDetail().getHostedQueues().size());

            Assert.assertEquals(queueNum, config3.getMappingDetail().getHostedQueues().size());

        }
        {
            Set<String> topics =  new HashSet<>(brokerController1.getTopicConfigManager().getTopicConfigTable().keySet());
            topics.remove(topic);
            brokerController1.getMessageStore().cleanUnusedTopic(topics);
            brokerController2.getMessageStore().cleanUnusedTopic(topics);
            for (int i = 0; i < 10; i++) {
                for (BrokerController brokerController: brokerControllerList) {
                    brokerController.getTopicQueueMappingCleanService().wakeup();
                }
                Thread.sleep(100);
            }

            Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(BROKER_NUM, brokerConfigMap.size());
            TopicConfigAndQueueMapping config1 = brokerConfigMap.get(BROKER1_NAME);
            TopicConfigAndQueueMapping config2 = brokerConfigMap.get(BROKER2_NAME);
            TopicConfigAndQueueMapping config3 = brokerConfigMap.get(BROKER3_NAME);
            Assert.assertEquals(0, config1.getMappingDetail().getHostedQueues().size());
            Assert.assertEquals(queueNum, config2.getMappingDetail().getHostedQueues().size());
            Assert.assertEquals(queueNum, config3.getMappingDetail().getHostedQueues().size());
            //The first leader will clear it
            for (List<LogicQueueMappingItem> items : config1.getMappingDetail().getHostedQueues().values()) {
                Assert.assertEquals(3, items.size());
            }
            //The second leader do nothing
            for (List<LogicQueueMappingItem> items : config3.getMappingDetail().getHostedQueues().values()) {
                Assert.assertEquals(1, items.size());
            }
        }
    }


    @Test
    public void testRemappingWithNegativeLogicOffset() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        int queueNum = 10;
        int msgEachQueue = 100;
        //create and send
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER1_NAME);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 0);
        }

        //remapping the static topic with -1 logic offset
        {
            Set<String> targetBrokers = ImmutableSet.of(BROKER2_NAME);
            MQAdminTestUtils.remappingStaticTopicWithNegativeLogicOffset(topic, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
            for (TopicQueueMappingOne mappingOne: globalIdMap.values()) {
                Assert.assertEquals(BROKER2_NAME, mappingOne.getBname());
                Assert.assertEquals(-1, mappingOne.getItems().get(mappingOne.getItems().size() - 1).getLogicOffset());
            }
            //leave the time to refresh the metadata
            awaitRefreshStaticTopicMetadata(3000, topic, producer.getProducer(), null, defaultMQAdminExt);
            //here the gen should be 0
            sendMessagesAndCheck(producer, targetBrokers, topic, queueNum, msgEachQueue, 0);
        }
    }


    @After
    public void tearDown() {
        System.setProperty("rocketmq.client.rebalance.waitInterval", "20000");
        super.shutdown();
    }

}
