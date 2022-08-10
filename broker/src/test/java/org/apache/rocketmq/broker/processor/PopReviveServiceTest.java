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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PopReviveServiceTest {

    private static final String REVIVE_TOPIC = PopAckConstants.REVIVE_TOPIC + "test";
    private static final int REVIVE_QUEUE_ID = 0;
    private static final String GROUP = "group";
    private static final String TOPIC = "topic";
    private static final SocketAddress STORE_HOST = RemotingUtil.string2SocketAddress("127.0.0.1:8080");

    @Mock
    private MessageStore messageStore;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private MessageStoreConfig messageStoreConfig;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private TimerMessageStore timerMessageStore;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;
    @Mock
    private BrokerController brokerController;

    private BrokerConfig brokerConfig;
    private PopReviveService popReviveService;

    @Before
    public void before() {
        brokerConfig = new BrokerConfig();

        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(messageStore.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(timerMessageStore.getReadBehind()).thenReturn(0L);
        when(timerMessageStore.getEnqueueBehind()).thenReturn(0L);

        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(new TopicConfig());
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(new SubscriptionGroupConfig());

        popReviveService = spy(new PopReviveService(brokerController, REVIVE_TOPIC, REVIVE_QUEUE_ID));
        popReviveService.setShouldRunPopRevive(true);
    }
    @Test
    public void testWhenAckMoreThanCk() throws Throwable {
        long maxReviveOffset = 4;

        when(consumerOffsetManager.queryOffset(PopAckConstants.REVIVE_GROUP, REVIVE_TOPIC, REVIVE_QUEUE_ID))
                .thenReturn(0L);
        List<MessageExt> reviveMessageExtList = new ArrayList<>();
        long basePopTime = System.currentTimeMillis();
        {
            // put a pair of ck and ack
            PopCheckPoint ck = buildPopCheckPoint(1, basePopTime, 1);
            reviveMessageExtList.add(buildCkMsg(ck));
            reviveMessageExtList.add(buildAckMsg(buildAckMsg(1, basePopTime), ck.getReviveTime(), 1, basePopTime));
        }
        {
            for (int i = 2; i <= maxReviveOffset; i++) {
                long popTime = basePopTime + i;
                PopCheckPoint ck = buildPopCheckPoint(i, popTime, i);
                reviveMessageExtList.add(buildAckMsg(buildAckMsg(i, popTime), ck.getReviveTime(), i, popTime));
            }
        }
        AtomicBoolean firstCall = new AtomicBoolean(true);
        doAnswer((Answer<List<MessageExt>>) mock -> {
            if (firstCall.get()) {
                firstCall.set(false);
                return reviveMessageExtList;
            }
            return null;
        }).when(popReviveService).getReviveMessage(anyLong(), anyInt());

        PopReviveService.ConsumeReviveObj consumeReviveObj = new PopReviveService.ConsumeReviveObj();
        popReviveService.consumeReviveMessage(consumeReviveObj);

        assertEquals(1, consumeReviveObj.map.size());

        AtomicLong committedOffset = new AtomicLong(-1);
        doAnswer(mock -> {
            committedOffset.set(mock.getArgument(4));
            return null;
        }).when(consumerOffsetManager).commitOffset(anyString(), anyString(), anyString(), anyInt(), anyLong());
        popReviveService.mergeAndRevive(consumeReviveObj);

        assertEquals(1, committedOffset.get());
    }

    public static PopCheckPoint buildPopCheckPoint(long startOffset, long popTime, long reviveOffset) {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setStartOffset(startOffset);
        ck.setPopTime(popTime);
        ck.setQueueId((byte) 0);
        ck.setCId(GROUP);
        ck.setTopic(TOPIC);
        ck.setNum((byte) 1);
        ck.setBitMap(0);
        ck.setReviveOffset(reviveOffset);
        ck.setInvisibleTime(1000);
        return ck;
    }

    public static AckMsg buildAckMsg(long offset, long popTime) {
        AckMsg ackMsg = new AckMsg();
        ackMsg.setAckOffset(offset);
        ackMsg.setStartOffset(offset);
        ackMsg.setConsumerGroup(GROUP);
        ackMsg.setTopic(TOPIC);
        ackMsg.setQueueId(0);
        ackMsg.setPopTime(popTime);

        return ackMsg;
    }

    public static MessageExtBrokerInner buildCkMsg(PopCheckPoint ck) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        msgInner.setTopic(REVIVE_TOPIC);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.charset));
        msgInner.setQueueId(REVIVE_QUEUE_ID);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(STORE_HOST);
        msgInner.setStoreHost(STORE_HOST);
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        msgInner.setQueueOffset(ck.getReviveOffset());

        return msgInner;
    }

    public static MessageExtBrokerInner buildAckMsg(AckMsg ackMsg, long deliverMs, long reviveOffset, long deliverTime) {
        MessageExtBrokerInner messageExtBrokerInner = buildAckInnerMessage(
                REVIVE_TOPIC,
                ackMsg,
                REVIVE_QUEUE_ID,
                STORE_HOST,
                deliverMs,
                PopMessageProcessor.genAckUniqueId(ackMsg)
        );
        messageExtBrokerInner.setQueueOffset(reviveOffset);
        messageExtBrokerInner.setDeliverTimeMs(deliverMs);
        messageExtBrokerInner.setStoreTimestamp(deliverTime);
        return messageExtBrokerInner;
    }

    public static MessageExtBrokerInner buildAckInnerMessage(String reviveTopic, AckMsg ackMsg, int reviveQid, SocketAddress host, long deliverMs, String ackUniqueId) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(host);
        msgInner.setStoreHost(host);
        msgInner.setDeliverTimeMs(deliverMs);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ackUniqueId);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }
}