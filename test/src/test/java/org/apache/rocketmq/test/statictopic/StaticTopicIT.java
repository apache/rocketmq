package org.apache.rocketmq.test.statictopic;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.common.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingOne;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils.getMappingDetailFromConfig;

@FixMethodOrder
public class StaticTopicIT extends BaseConf {

    private static Logger logger = Logger.getLogger(StaticTopicIT.class);
    private DefaultMQAdminExt defaultMQAdminExt;

    @Before
    public void setUp() throws Exception {
        System.setProperty("rocketmq.client.rebalance.waitInterval", "500");
        defaultMQAdminExt = getAdmin(nsAddr);
        waitBrokerRegistered(nsAddr, clusterName);
        defaultMQAdminExt.start();
    }


    @Test
    public void testNoTargetBrokers() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        int queueNum = 10;
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker1Name);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(brokerNum, remoteBrokerConfigMap.size());
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
            TopicConfigAndQueueMapping configMapping = remoteBrokerConfigMap.get(broker2Name);
            Assert.assertEquals(0, configMapping.getWriteQueueNums());
            Assert.assertEquals(0, configMapping.getReadQueueNums());
            Assert.assertEquals(0, configMapping.getMappingDetail().getHostedQueues().size());
        }

        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker2Name);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(brokerNum, remoteBrokerConfigMap.size());
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
        }

    }


    @Test
    public void testCreateProduceConsumeStaticTopic() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(nsAddr, topic);
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListener());

        int queueNum = 10;
        int msgEachQueue = 100;
        //create static topic
        Map<String, TopicConfigAndQueueMapping> localBrokerConfigMap = MQAdminTestUtils.createStaticTopic(topic, queueNum, getBrokers(), defaultMQAdminExt);
        //check the static topic config
        {
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(brokerNum, remoteBrokerConfigMap.size());
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
        //check the route data
        List<MessageQueue> messageQueueList = producer.getMessageQueue();
        Assert.assertEquals(queueNum, messageQueueList.size());
        producer.setDebug(true);
        for (int i = 0; i < queueNum; i++) {
            MessageQueue messageQueue = messageQueueList.get(i);
            Assert.assertEquals(topic, messageQueue.getTopic());
            Assert.assertEquals(i, messageQueue.getQueueId());
            Assert.assertEquals(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, messageQueue.getBrokerName());
        }
        //send and consume the msg
        for(MessageQueue messageQueue: messageQueueList) {
            producer.send(msgEachQueue, messageQueue);
        }
        //leave the time to build the cq
        Thread.sleep(500);
        for(MessageQueue messageQueue: messageQueueList) {
            Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
            Assert.assertEquals(msgEachQueue, defaultMQAdminExt.maxOffset(messageQueue));
        }
        Assert.assertEquals(msgEachQueue * queueNum, producer.getAllOriginMsg().size());
        Assert.assertEquals(0, producer.getSendErrorMsg().size());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), 3000);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer.getListener().getAllMsgBody()))
                .containsExactlyElementsIn(producer.getAllMsgBody());
        Map<Integer, List<MessageExt>> messagesByQueue = computeMessageByQueue(consumer.getListener().getAllOriginMsg());
        Assert.assertEquals(queueNum, messagesByQueue.size());
        for (int i = 0; i < queueNum; i++) {
            List<MessageExt> messageExts = messagesByQueue.get(i);
            Assert.assertEquals(msgEachQueue, messageExts.size());
            for (int j = 0; j < msgEachQueue; j++) {
                Assert.assertEquals(j, messageExts.get(j).getQueueOffset());
            }
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
        for (List<MessageExt> msgEachQueue: messagesByQueue.values()) {
            Collections.sort(msgEachQueue, new Comparator<MessageExt>() {
                @Override
                public int compare(MessageExt o1, MessageExt o2) {
                    return (int) (o1.getQueueOffset() - o2.getQueueOffset());
                }
            });
        }
        return messagesByQueue;
    }

    @Test
    public void testDoubleReadCheckConsumerOffset() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        String group = initConsumerGroup();
        RMQNormalProducer producer = getProducer(nsAddr, topic);

        RMQNormalConsumer consumer = getConsumer(nsAddr, group, topic, "*", new RMQNormalListener());

        //System.out.printf("Group:%s\n", consumer.getConsumerGroup());
        //System.out.printf("Topic:%s\n", topic);

        int queueNum = 10;
        int msgEachQueue = 100;
        //create static topic
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker1Name);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
        }
        //produce the messages
        {
            List<MessageQueue> messageQueueList = producer.getMessageQueue();
            for(MessageQueue messageQueue: messageQueueList) {
                producer.send(msgEachQueue, messageQueue);
            }
            Assert.assertEquals(0, producer.getSendErrorMsg().size());
            Assert.assertEquals(msgEachQueue * queueNum, producer.getAllMsgBody().size());
        }

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), 3000);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer.getListener().getAllMsgBody()))
                .containsExactlyElementsIn(producer.getAllMsgBody());
        producer.shutdown();
        consumer.shutdown();

        //remapping the static topic
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker2Name);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);

        }
        //make the metadata
        Thread.sleep(500);
        //System.out.printf("Group:%s\n", consumer.getConsumerGroup());

        {
            producer = getProducer(nsAddr, topic);
            ClientMetadata clientMetadata = MQAdminUtils.getBrokerAndTopicMetadata(topic, defaultMQAdminExt);
            //just refresh the metadata
            List<MessageQueue> messageQueueList = producer.getMessageQueue();
            for(MessageQueue messageQueue: messageQueueList) {
                producer.send(msgEachQueue, messageQueue);
                Assert.assertEquals(broker2Name, clientMetadata.getBrokerNameFromMessageQueue(messageQueue));
            }
            Assert.assertEquals(0, producer.getSendErrorMsg().size());
            Assert.assertEquals(msgEachQueue * queueNum, producer.getAllMsgBody().size());
            for(MessageQueue messageQueue: messageQueueList) {
                Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
                Assert.assertEquals(msgEachQueue + TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, defaultMQAdminExt.maxOffset(messageQueue));
            }
            //leave the time to build the cq
            Thread.sleep(100);
        }
        {
            consumer = getConsumer(nsAddr, group, topic, "*", new RMQNormalListener());
            consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), 6000);
            //System.out.printf("Consume %d\n", consumer.getListener().getAllMsgBody().size());
            assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                    consumer.getListener().getAllMsgBody()))
                    .containsExactlyElementsIn(producer.getAllMsgBody());

            Map<Integer, List<MessageExt>> messagesByQueue = computeMessageByQueue(consumer.getListener().getAllOriginMsg());

            Assert.assertEquals(queueNum, messagesByQueue.size());
            for (int i = 0; i < queueNum; i++) {
                List<MessageExt> messageExts = messagesByQueue.get(i);
                Assert.assertEquals(msgEachQueue, messageExts.size());
                for (int j = 0; j < msgEachQueue; j++) {
                    Assert.assertEquals(j + TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, messageExts.get(j).getQueueOffset());
                }
            }
        }
    }

    @Test
    public void testRemappingProduceConsumeStaticTopic() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(nsAddr, topic);
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListener());


        int queueNum = 10;
        int msgEachQueue = 100;
        //create static topic
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker1Name);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
        }
        //System.out.printf("%s %s\n", broker1Name, clientMetadata.findMasterBrokerAddr(broker1Name));
        //System.out.printf("%s %s\n", broker2Name, clientMetadata.findMasterBrokerAddr(broker2Name));

        //produce the messages
        {
            List<MessageQueue> messageQueueList = producer.getMessageQueue();
            for (int i = 0; i < queueNum; i++) {
                MessageQueue messageQueue = messageQueueList.get(i);
                Assert.assertEquals(i, messageQueue.getQueueId());
                Assert.assertEquals(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, messageQueue.getBrokerName());
            }
            for(MessageQueue messageQueue: messageQueueList) {
                producer.send(msgEachQueue, messageQueue);
            }
            Assert.assertEquals(0, producer.getSendErrorMsg().size());
            //leave the time to build the cq
            Thread.sleep(100);
            for(MessageQueue messageQueue: messageQueueList) {
                //Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
                Assert.assertEquals(msgEachQueue, defaultMQAdminExt.maxOffset(messageQueue));
            }
        }

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), 3000);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer.getListener().getAllMsgBody()))
                .containsExactlyElementsIn(producer.getAllMsgBody());

        //remapping the static topic
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker2Name);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);

            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
            for (TopicQueueMappingOne mappingOne: globalIdMap.values()) {
                Assert.assertEquals(broker2Name, mappingOne.getBname());
            }
        }
        //leave the time to refresh the metadata
        Thread.sleep(500);
        producer.setDebug(true);
        {
            ClientMetadata clientMetadata = MQAdminUtils.getBrokerAndTopicMetadata(topic, defaultMQAdminExt);
            List<MessageQueue> messageQueueList = producer.getMessageQueue();
            for (int i = 0; i < queueNum; i++) {
                MessageQueue messageQueue = messageQueueList.get(i);
                Assert.assertEquals(i, messageQueue.getQueueId());
                Assert.assertEquals(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, messageQueue.getBrokerName());
                String destBrokerName = clientMetadata.getBrokerNameFromMessageQueue(messageQueue);
                Assert.assertEquals(destBrokerName, broker2Name);
            }

            for(MessageQueue messageQueue: messageQueueList) {
                producer.send(msgEachQueue, messageQueue);
            }
            Assert.assertEquals(0, producer.getSendErrorMsg().size());
            //leave the time to build the cq
            Thread.sleep(100);
            for(MessageQueue messageQueue: messageQueueList) {
                Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
                Assert.assertEquals(msgEachQueue + TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, defaultMQAdminExt.maxOffset(messageQueue));
            }
        }
        {
            consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), 30000);
            assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                    consumer.getListener().getAllMsgBody()))
                    .containsExactlyElementsIn(producer.getAllMsgBody());
            Map<Integer, List<MessageExt>> messagesByQueue = computeMessageByQueue(consumer.getListener().getAllOriginMsg());
            Assert.assertEquals(queueNum, messagesByQueue.size());
            for (int i = 0; i < queueNum; i++) {
                List<MessageExt> messageExts = messagesByQueue.get(i);
                Assert.assertEquals(msgEachQueue * 2, messageExts.size());
                for (int j = 0; j < msgEachQueue; j++) {
                    Assert.assertEquals(j, messageExts.get(j).getQueueOffset());
                }
                for (int j = msgEachQueue; j < msgEachQueue * 2; j++) {
                    Assert.assertEquals(j + TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE - msgEachQueue, messageExts.get(j).getQueueOffset());
                }
            }
        }
    }


    public void sendMessagesAndCheck(RMQNormalProducer producer, String broker, String topic, int queueNum, int msgEachQueue, long baseOffset) throws Exception {
        ClientMetadata clientMetadata = MQAdminUtils.getBrokerAndTopicMetadata(topic, defaultMQAdminExt);
        List<MessageQueue> messageQueueList = producer.getMessageQueue();
        Assert.assertEquals(queueNum, messageQueueList.size());
        for (int i = 0; i < queueNum; i++) {
            MessageQueue messageQueue = messageQueueList.get(i);
            Assert.assertEquals(i, messageQueue.getQueueId());
            Assert.assertEquals(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, messageQueue.getBrokerName());
            String destBrokerName = clientMetadata.getBrokerNameFromMessageQueue(messageQueue);
            Assert.assertEquals(destBrokerName, broker);
        }

        for(MessageQueue messageQueue: messageQueueList) {
            producer.send(msgEachQueue, messageQueue);
        }
        Assert.assertEquals(0, producer.getSendErrorMsg().size());
        //leave the time to build the cq
        Thread.sleep(100);
        for(MessageQueue messageQueue: messageQueueList) {
            Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
            Assert.assertEquals(msgEachQueue + baseOffset, defaultMQAdminExt.maxOffset(messageQueue));
        }
    }


    @Test
    public void testRemappingAndClear() throws Exception {
        String topic = "static" + MQRandomUtils.getRandomTopic();
        RMQNormalProducer producer = getProducer(nsAddr, topic);
        int queueNum = 10;
        int msgEachQueue = 100;
        //create to broker1Name
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker1Name);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
            //leave the time to refresh the metadata
            Thread.sleep(500);
            sendMessagesAndCheck(producer, broker1Name, topic, queueNum, msgEachQueue, 0L);
        }

        //remapping to broker2Name
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker2Name);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            //leave the time to refresh the metadata
            Thread.sleep(500);
            sendMessagesAndCheck(producer, broker2Name, topic, queueNum, msgEachQueue, TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE);
        }

        //remapping to broker3Name
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker3Name);
            MQAdminTestUtils.remappingStaticTopic(topic, targetBrokers, defaultMQAdminExt);
            //leave the time to refresh the metadata
            Thread.sleep(500);
            sendMessagesAndCheck(producer, broker3Name, topic, queueNum, msgEachQueue, TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE * 2);
        }

        // 1 -> 2 -> 3, currently 1 should not has any mappings

        {
            for (int i = 0; i < 10; i++) {
                for (BrokerController brokerController: brokerControllerList) {
                    brokerController.getTopicQueueMappingCleanService().wakeup();
                }
                Thread.sleep(100);
            }
            Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            Assert.assertEquals(brokerNum, brokerConfigMap.size());
            TopicConfigAndQueueMapping config1 = brokerConfigMap.get(broker1Name);
            TopicConfigAndQueueMapping config2 = brokerConfigMap.get(broker2Name);
            TopicConfigAndQueueMapping config3 = brokerConfigMap.get(broker3Name);
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
            Assert.assertEquals(brokerNum, brokerConfigMap.size());
            TopicConfigAndQueueMapping config1 = brokerConfigMap.get(broker1Name);
            TopicConfigAndQueueMapping config2 = brokerConfigMap.get(broker2Name);
            TopicConfigAndQueueMapping config3 = brokerConfigMap.get(broker3Name);
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
        RMQNormalProducer producer = getProducer(nsAddr, topic);
        int queueNum = 10;
        int msgEachQueue = 100;
        //create static topic
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker1Name);
            MQAdminTestUtils.createStaticTopic(topic, queueNum, targetBrokers, defaultMQAdminExt);
        }
        //System.out.printf("%s %s\n", broker1Name, clientMetadata.findMasterBrokerAddr(broker1Name));
        //System.out.printf("%s %s\n", broker2Name, clientMetadata.findMasterBrokerAddr(broker2Name));

        //produce the messages
        {
            List<MessageQueue> messageQueueList = producer.getMessageQueue();
            for (int i = 0; i < queueNum; i++) {
                MessageQueue messageQueue = messageQueueList.get(i);
                Assert.assertEquals(i, messageQueue.getQueueId());
                Assert.assertEquals(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, messageQueue.getBrokerName());
            }
            for(MessageQueue messageQueue: messageQueueList) {
                producer.send(msgEachQueue, messageQueue);
            }
            Assert.assertEquals(0, producer.getSendErrorMsg().size());
            //leave the time to build the cq
            Thread.sleep(100);
            for(MessageQueue messageQueue: messageQueueList) {
                //Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
                Assert.assertEquals(msgEachQueue, defaultMQAdminExt.maxOffset(messageQueue));
            }
        }

        //remapping the static topic with -1 logic offset
        {
            Set<String> targetBrokers = new HashSet<>();
            targetBrokers.add(broker2Name);
            MQAdminTestUtils.remappingStaticTopicWithNegativeLogicOffset(topic, targetBrokers, defaultMQAdminExt);
            Map<String, TopicConfigAndQueueMapping> remoteBrokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);

            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, remoteBrokerConfigMap);
            Map<Integer, TopicQueueMappingOne>  globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(getMappingDetailFromConfig(remoteBrokerConfigMap.values())), false, true);
            Assert.assertEquals(queueNum, globalIdMap.size());
            for (TopicQueueMappingOne mappingOne: globalIdMap.values()) {
                Assert.assertEquals(broker2Name, mappingOne.getBname());
                Assert.assertEquals(-1, mappingOne.getItems().get(mappingOne.getItems().size() - 1).getLogicOffset());
            }
        }
        //leave the time to refresh the metadata
        Thread.sleep(500);
        producer.setDebug(true);
        {
            ClientMetadata clientMetadata = MQAdminUtils.getBrokerAndTopicMetadata(topic, defaultMQAdminExt);
            List<MessageQueue> messageQueueList = producer.getMessageQueue();
            for (int i = 0; i < queueNum; i++) {
                MessageQueue messageQueue = messageQueueList.get(i);
                Assert.assertEquals(i, messageQueue.getQueueId());
                String destBrokerName = clientMetadata.getBrokerNameFromMessageQueue(messageQueue);
                Assert.assertEquals(destBrokerName, broker2Name);
            }

            for(MessageQueue messageQueue: messageQueueList) {
                producer.send(msgEachQueue, messageQueue);
            }
            Assert.assertEquals(0, producer.getSendErrorMsg().size());
            Assert.assertEquals(queueNum * msgEachQueue * 2, producer.getAllOriginMsg().size());
            //leave the time to build the cq
            Thread.sleep(100);
            for(MessageQueue messageQueue: messageQueueList) {
                Assert.assertEquals(0, defaultMQAdminExt.minOffset(messageQueue));
                //the max offset should still be msgEachQueue
                Assert.assertEquals(msgEachQueue, defaultMQAdminExt.maxOffset(messageQueue));
            }
        }
    }


    @After
    public void tearDown() {
        System.setProperty("rocketmq.client.rebalance.waitInterval", "20000");
        super.shutdown();
    }

}
