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

package org.apache.rocketmq.test.offset;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.DefaultMessageFilter;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQSqlConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQBlockListener;
import org.apache.rocketmq.test.message.MessageQueueMsg;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LagCalculationIT extends BaseConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(LagCalculationIT.class);
    private RMQNormalProducer producer = null;
    private RMQNormalConsumer consumer = null;
    private String topic = null;
    private RMQBlockListener blockListener = null;

    @Before
    public void setUp() {
        topic = initTopic();
        LOGGER.info(String.format("use topic: %s;", topic));
        for (BrokerController controller : brokerControllerList) {
            controller.getBrokerConfig().setLongPollingEnable(false);
            controller.getBrokerConfig().setShortPollingTimeMills(500);
        }
        producer = getProducer(NAMESRV_ADDR, topic);
        blockListener = new RMQBlockListener(false);
        consumer = getConsumer(NAMESRV_ADDR, topic, "*", blockListener);
    }

    @After
    public void tearDown() {
        for (BrokerController controller : brokerControllerList) {
            controller.getBrokerConfig().setLongPollingEnable(true);
            controller.getBrokerConfig().setShortPollingTimeMills(1000);
            controller.getBrokerConfig().setUseStaticSubscription(false);
        }
        shutdown();
    }

    private Pair<Long, Long> getLag(List<MessageQueue> mqs) {
        long lag = 0;
        long pullLag = 0;
        for (BrokerController controller : brokerControllerList) {
            ConsumeStats consumeStats = MQAdminTestUtils.examineConsumeStats(controller.getBrokerAddr(), topic, consumer.getConsumerGroup());
            Map<MessageQueue, OffsetWrapper> offsetTable = consumeStats.getOffsetTable();
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());

                    long consumerOffset = controller.getConsumerOffsetManager().queryOffset(consumer.getConsumerGroup(),
                        topic, mq.getQueueId());
                    long pullOffset =
                        controller.getConsumerOffsetManager().queryPullOffset(consumer.getConsumerGroup(),
                            topic, mq.getQueueId());
                    OffsetWrapper offsetWrapper = offsetTable.get(mq);
                    assertEquals(brokerOffset, offsetWrapper.getBrokerOffset());
                    if (offsetWrapper.getConsumerOffset() != consumerOffset || offsetWrapper.getPullOffset() != pullOffset) {
                        return new Pair<>(-1L, -1L);
                    }
                    lag += brokerOffset - consumerOffset;
                    pullLag += brokerOffset - pullOffset;
                }
            }
        }
        return new Pair<>(lag, pullLag);
    }

    public void waitForFullyDispatched() {
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            for (BrokerController controller : brokerControllerList) {
                if (controller.getMessageStore().dispatchBehindBytes() != 0) {
                    return false;
                }
            }
            return true;
        });
    }

    @Test
    public void testCalculateLag() {
        int msgSize = 10;
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize);

        producer.send(mqMsgs.getMsgsWithMQ());
        waitForFullyDispatched();
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        consumer.getConsumer().getDefaultMQPushConsumerImpl().persistConsumerOffset();

        // wait for consume all msgs
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            Pair<Long, Long> lag = getLag(mqs);
            return lag.getObject1() == 0 && lag.getObject2() == 0;
        });

        blockListener.setBlock(true);
        consumer.clearMsg();
        producer.clearMsg();
        producer.send(mqMsgs.getMsgsWithMQ());
        waitForFullyDispatched();

        // wait for pull all msgs
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            Pair<Long, Long> lag = getLag(mqs);
            return lag.getObject1() == producer.getAllMsgBody().size() && lag.getObject2() == 0;
        });

        blockListener.setBlock(false);
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        consumer.shutdown();
        producer.clearMsg();
        producer.send(mqMsgs.getMsgsWithMQ());
        waitForFullyDispatched();

        Pair<Long, Long> lag = getLag(mqs);
        assertEquals(producer.getAllMsgBody().size(), (long) lag.getObject1());
        assertEquals(producer.getAllMsgBody().size(), (long) lag.getObject2());
    }

    @Test
    public void testEstimateLag() throws Exception {
        int msgNoTagSize = 80;
        int msgWithTagSize = 20;
        int repeat = 2;
        String tag = "TAG_FOR_TEST_ESTIMATE";
        String sql = "TAGS = 'TAG_FOR_TEST_ESTIMATE' And value < " + repeat / 2;
        MessageSelector selector = MessageSelector.bySql(sql);
        RMQBlockListener sqlListener = new RMQBlockListener(true);
        RMQSqlConsumer sqlConsumer = ConsumerFactory.getRMQSqlConsumer(NAMESRV_ADDR, initConsumerGroup(), topic, selector, sqlListener);
        RMQBlockListener tagListener = new RMQBlockListener(true);
        RMQNormalConsumer tagConsumer = getConsumer(NAMESRV_ADDR, topic, tag, tagListener);

        //init subscriptionData & consumerFilterData for sql
        SubscriptionData subscriptionData = FilterAPI.build(topic, sql, ExpressionType.SQL92);
        for (BrokerController controller : brokerControllerList) {
            controller.getConsumerFilterManager().register(topic, sqlConsumer.getConsumerGroup(), sql, ExpressionType.SQL92, subscriptionData.getSubVersion());
        }

        // wait for building filter data
        await().atMost(5, TimeUnit.SECONDS).until(() -> sqlListener.isBlocked() && tagListener.isBlocked());

        List<MessageQueue> mqs = producer.getMessageQueue();
        for (int i = 0; i < repeat; i++) {
            MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgNoTagSize);
            Map<MessageQueue, List<Object>> msgMap = mqMsgs.getMsgsWithMQ();
            mqMsgs = new MessageQueueMsg(mqs, msgWithTagSize, tag);
            Map<MessageQueue, List<Object>> msgWithTagMap = mqMsgs.getMsgsWithMQ();
            int finalI = i;
            msgMap.forEach((mq, msgList) -> {
                List<Object> msgWithTagList = msgWithTagMap.get(mq);
                for (Object o : msgWithTagList) {
                    ((Message) o).putUserProperty("value", String.valueOf(finalI));
                }
                msgList.addAll(msgWithTagList);
                Collections.shuffle(msgList);
            });
            producer.send(msgMap);
        }

        // test lag estimation for tag consumer
        for (BrokerController controller : brokerControllerList) {
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());
                    long estimateMessageCount = controller.getMessageStore()
                        .estimateMessageCount(topic, mq.getQueueId(), 0, brokerOffset,
                            new DefaultMessageFilter(FilterAPI.buildSubscriptionData(topic, tag)));
                    assertEquals(repeat * msgWithTagSize, estimateMessageCount);
                }
            }
        }

        // test lag estimation for sql consumer
        for (BrokerController controller : brokerControllerList) {
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());
                    ConsumerFilterData consumerFilterData = controller.getConsumerFilterManager().get(topic, sqlConsumer.getConsumerGroup());
                    long estimateMessageCount = controller.getMessageStore()
                        .estimateMessageCount(topic, mq.getQueueId(), 0, brokerOffset,
                            new ExpressionMessageFilter(subscriptionData, consumerFilterData, controller.getConsumerFilterManager()));
                    assertEquals(repeat / 2 * msgWithTagSize, estimateMessageCount);
                }
            }
        }

        sqlConsumer.shutdown();
        tagConsumer.shutdown();
    }

    @Test
    public void testEstimateLagWhenUseStaticSubscription() throws Exception {
        for (BrokerController controller : brokerControllerList) {
            controller.getBrokerConfig().setUseStaticSubscription(true);
        }
        int msgNoTagSize = 80;
        int msgWithTagSize = 20;
        int repeat = 2;
        String tag = "TAG_FOR_TEST_ESTIMATE";
        String sql = "TAGS = 'TAG_FOR_TEST_ESTIMATE' And value < " + repeat / 2;
        MessageSelector selector = MessageSelector.bySql(sql);
        RMQBlockListener sqlListener = new RMQBlockListener(true);
        RMQSqlConsumer sqlConsumer = ConsumerFactory.getRMQSqlConsumer(NAMESRV_ADDR, initConsumerGroup(), topic, selector, sqlListener);
        RMQBlockListener tagListener = new RMQBlockListener(true);
        RMQNormalConsumer tagConsumer = getConsumer(NAMESRV_ADDR, topic, tag, tagListener);
        DefaultMQAdminExt admin = getAdmin(NAMESRV_ADDR);
        admin.start();

        //init subscriptionData & consumerFilterData for sql
        SubscriptionData subscriptionData = FilterAPI.build(topic, sql, ExpressionType.SQL92);

        Set<String> masterSet =
            CommandUtil.fetchMasterAddrByClusterName(admin, CLUSTER_NAME);
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(sqlConsumer.getConsumerGroup());
        SimpleSubscriptionData simpleSubscriptionData = new SimpleSubscriptionData(topic, "SQL92", sql, Integer.MAX_VALUE);
        subscriptionGroupConfig.setSubscriptionDataSet(Sets.newHashSet(simpleSubscriptionData));
        for (String addr : masterSet) {
            admin.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
        }

        // wait for building filter data
        await().atMost(5, TimeUnit.SECONDS).until(() -> sqlListener.isBlocked() && tagListener.isBlocked());

        List<MessageQueue> mqs = producer.getMessageQueue();
        for (int i = 0; i < repeat; i++) {
            MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgNoTagSize);
            Map<MessageQueue, List<Object>> msgMap = mqMsgs.getMsgsWithMQ();
            mqMsgs = new MessageQueueMsg(mqs, msgWithTagSize, tag);
            Map<MessageQueue, List<Object>> msgWithTagMap = mqMsgs.getMsgsWithMQ();
            int finalI = i;
            msgMap.forEach((mq, msgList) -> {
                List<Object> msgWithTagList = msgWithTagMap.get(mq);
                for (Object o : msgWithTagList) {
                    ((Message) o).putUserProperty("value", String.valueOf(finalI));
                }
                msgList.addAll(msgWithTagList);
                Collections.shuffle(msgList);
            });
            producer.send(msgMap);
        }

        // test lag estimation for tag consumer
        for (BrokerController controller : brokerControllerList) {
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());
                    long estimateMessageCount = controller.getMessageStore()
                        .estimateMessageCount(topic, mq.getQueueId(), 0, brokerOffset,
                            new DefaultMessageFilter(FilterAPI.buildSubscriptionData(topic, tag)));
                    assertEquals(repeat * msgWithTagSize, estimateMessageCount);
                }
            }
        }

        // test lag estimation for sql consumer
        for (BrokerController controller : brokerControllerList) {
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());
                    long estimateMessageCount = controller.getMessageStore()
                        .estimateMessageCount(topic, mq.getQueueId(), 0, brokerOffset,
                            new ExpressionMessageFilter(subscriptionData, controller.getConsumerFilterManager().get(topic, sqlConsumer.getConsumerGroup()), controller.getConsumerFilterManager()));
                    assertEquals(repeat / 2 * msgWithTagSize, estimateMessageCount);
                }
            }
        }

        sqlConsumer.shutdown();
        tagConsumer.shutdown();
    }


    @Test
    public void testEstimateLagWhenUseStaticSubscription1() throws Exception {
        for (BrokerController controller : brokerControllerList) {
            controller.getBrokerConfig().setUseStaticSubscription(true);
        }
        int msgNoTagSize = 80;
        int msgWithTagSize = 20;
        int repeat = 2;
        String tag = "TAG_FOR_TEST_ESTIMATE";
        String sql = "TAGS = 'TAG_FOR_TEST_ESTIMATE' And value < " + 0;
        MessageSelector selector = MessageSelector.bySql(sql);
        RMQBlockListener sqlListener = new RMQBlockListener(true);
        RMQSqlConsumer sqlConsumer = ConsumerFactory.getRMQSqlConsumer(NAMESRV_ADDR, initConsumerGroup(), topic, selector, sqlListener);
        RMQBlockListener tagListener = new RMQBlockListener(true);
        RMQNormalConsumer tagConsumer = getConsumer(NAMESRV_ADDR, topic, tag, tagListener);
        DefaultMQAdminExt admin = getAdmin(NAMESRV_ADDR);
        admin.start();

        //init subscriptionData & consumerFilterData for sql
        SubscriptionData subscriptionData = FilterAPI.build(topic, sql, ExpressionType.SQL92);

        Set<String> masterSet =
            CommandUtil.fetchMasterAddrByClusterName(admin, CLUSTER_NAME);
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(sqlConsumer.getConsumerGroup());
        SimpleSubscriptionData simpleSubscriptionData = new SimpleSubscriptionData(topic, "SQL92", sql, Integer.MAX_VALUE);
        subscriptionGroupConfig.setSubscriptionDataSet(Sets.newHashSet(simpleSubscriptionData));
        for (String addr : masterSet) {
            admin.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
        }

        // wait for building filter data
        await().atMost(5, TimeUnit.SECONDS).until(() -> sqlListener.isBlocked() && tagListener.isBlocked());

        List<MessageQueue> mqs = producer.getMessageQueue();
        for (int i = 0; i < repeat; i++) {
            MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgNoTagSize);
            Map<MessageQueue, List<Object>> msgMap = mqMsgs.getMsgsWithMQ();
            mqMsgs = new MessageQueueMsg(mqs, msgWithTagSize, tag);
            Map<MessageQueue, List<Object>> msgWithTagMap = mqMsgs.getMsgsWithMQ();
            int finalI = i;
            msgMap.forEach((mq, msgList) -> {
                List<Object> msgWithTagList = msgWithTagMap.get(mq);
                for (Object o : msgWithTagList) {
                    ((Message) o).putUserProperty("value", String.valueOf(finalI));
                }
                msgList.addAll(msgWithTagList);
                Collections.shuffle(msgList);
            });
            producer.send(msgMap);
        }

        // test lag estimation for tag consumer
        for (BrokerController controller : brokerControllerList) {
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());
                    long estimateMessageCount = controller.getMessageStore()
                        .estimateMessageCount(topic, mq.getQueueId(), 0, brokerOffset,
                            new DefaultMessageFilter(FilterAPI.buildSubscriptionData(topic, tag)));
                    assertEquals(repeat * msgWithTagSize, estimateMessageCount);
                }
            }
        }

        // test lag estimation for sql consumer
        for (BrokerController controller : brokerControllerList) {
            for (MessageQueue mq : mqs) {
                if (mq.getBrokerName().equals(controller.getBrokerConfig().getBrokerName())) {
                    long brokerOffset = controller.getMessageStore().getMaxOffsetInQueue(topic, mq.getQueueId());
                    long estimateMessageCount = controller.getMessageStore()
                        .estimateMessageCount(topic, mq.getQueueId(), 0, brokerOffset,
                            new ExpressionMessageFilter(subscriptionData, controller.getConsumerFilterManager().get(topic, sqlConsumer.getConsumerGroup()), controller.getConsumerFilterManager()));
                    assertEquals(0, estimateMessageCount);
                }
            }
        }

        sqlConsumer.shutdown();
        tagConsumer.shutdown();
    }
}
