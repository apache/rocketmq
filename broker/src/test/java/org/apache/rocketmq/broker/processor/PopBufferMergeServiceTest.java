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

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PopBufferMergeServiceTest {

    @Mock
    private BrokerController brokerController;

    private PopMessageProcessor popMessageProcessor;

    @Mock
    private ScheduleMessageService scheduleMessageService;

    @Mock
    private TopicConfigManager topicConfigManager;

    @Mock
    private ConsumerManager consumerManager;

    @Mock
    private DefaultMessageStore messageStore;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    private String defaultGroup = "defaultGroup";

    private String defaultTopic = "defaultTopic";

    private PopBufferMergeService popBufferMergeService;

    @Mock
    private BrokerConfig brokerConfig;

    @Mock
    private EscapeBridge escapeBridge;

    @Before
    public void init() throws Exception {
        when(brokerConfig.getBrokerIP1()).thenReturn("127.0.0.1");
        when(brokerConfig.isEnablePopBufferMerge()).thenReturn(true);
        when(brokerConfig.getPopCkStayBufferTime()).thenReturn(10 * 1000);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getEscapeBridge()).thenReturn(escapeBridge);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getScheduleMessageService()).thenReturn(scheduleMessageService);
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        popMessageProcessor = new PopMessageProcessor(brokerController);
        popBufferMergeService = new PopBufferMergeService(brokerController, popMessageProcessor);
        FieldUtils.writeDeclaredField(popBufferMergeService, "brokerController", brokerController, true);
        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        topicConfigTable.put(defaultTopic, new TopicConfig());
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicConfigTable);
    }

    @Test(timeout = 15_000)
    public void testBasic() throws Exception {
        // This test case fails on Windows in CI pipeline
        // Disable it for later fix
        Assume.assumeFalse(MixAll.isWindows());
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        int msgCnt = 1;
        ck.setNum((byte) msgCnt);
        long popTime = System.currentTimeMillis() - 1000;
        ck.setPopTime(popTime);
        int invisibleTime = 30_000;
        ck.setInvisibleTime(invisibleTime);
        int offset = 100;
        ck.setStartOffset(offset);
        ck.setCId(defaultGroup);
        ck.setTopic(defaultTopic);
        int queueId = 0;
        ck.setQueueId(queueId);

        int reviveQid = 0;
        long nextBeginOffset = 101L;
        long ackOffset = offset;
        AckMsg ackMsg = new AckMsg();
        ackMsg.setAckOffset(ackOffset);
        ackMsg.setStartOffset(offset);
        ackMsg.setConsumerGroup(defaultGroup);
        ackMsg.setTopic(defaultTopic);
        ackMsg.setQueueId(queueId);
        ackMsg.setPopTime(popTime);
        try {
            assertThat(popBufferMergeService.addCk(ck, reviveQid, ackOffset, nextBeginOffset)).isTrue();
            assertThat(popBufferMergeService.getLatestOffset(defaultTopic, defaultGroup, queueId)).isEqualTo(nextBeginOffset);
            Thread.sleep(1000); // wait background threads of PopBufferMergeService run for some time
            assertThat(popBufferMergeService.addAk(reviveQid, ackMsg)).isTrue();
            assertThat(popBufferMergeService.getLatestOffset(defaultTopic, defaultGroup, queueId)).isEqualTo(nextBeginOffset);
        } finally {
            popBufferMergeService.shutdown(true);
        }
    }

    @Test
    public void testAddCkJustOffset_MergeKeyConflict() {
        PopCheckPoint point = mock(PopCheckPoint.class);
        String mergeKey = "testMergeKey";
        when(point.getTopic()).thenReturn(mergeKey);
        when(point.getCId()).thenReturn("");
        when(point.getQueueId()).thenReturn(0);
        when(point.getStartOffset()).thenReturn(0L);
        when(point.getPopTime()).thenReturn(0L);
        when(point.getBrokerName()).thenReturn("");
        popBufferMergeService.buffer.put(mergeKey + "000", mock(PopBufferMergeService.PopCheckPointWrapper.class));

        assertFalse(popBufferMergeService.addCkJustOffset(point, 0, 0, 0));
    }

    @Test
    public void testAddCkMock() {
        int queueId = 0;
        long startOffset = 100L;
        long invisibleTime = 30_000L;
        long popTime = System.currentTimeMillis();
        int reviveQueueId = 0;
        long nextBeginOffset = 101L;
        String brokerName = "brokerName";
        popBufferMergeService.addCkMock(defaultGroup, defaultTopic, queueId, startOffset, invisibleTime, popTime, reviveQueueId, nextBeginOffset, brokerName);
        verify(brokerConfig, times(1)).isEnablePopLog();
    }

    @Test
    public void testPutAckToStore() throws Exception {
        PopCheckPoint point = new PopCheckPoint();
        point.setStartOffset(100L);
        point.setCId("testGroup");
        point.setTopic("testTopic");
        point.setQueueId(1);
        point.setPopTime(System.currentTimeMillis());
        point.setBrokerName("testBroker");

        PopBufferMergeService.PopCheckPointWrapper pointWrapper = mock(PopBufferMergeService.PopCheckPointWrapper.class);
        when(pointWrapper.getCk()).thenReturn(point);
        when(pointWrapper.getReviveQueueId()).thenReturn(0);

        AtomicInteger toStoreBits = new AtomicInteger(0);
        when(pointWrapper.getToStoreBits()).thenReturn(toStoreBits);

        byte msgIndex = 0;
        AtomicInteger count = new AtomicInteger(0);

        EscapeBridge escapeBridge = mock(EscapeBridge.class);
        when(brokerController.getEscapeBridge()).thenReturn(escapeBridge);
        when(brokerController.getBrokerConfig().isAppendAckAsync()).thenReturn(false);

        when(escapeBridge.putMessageToSpecificQueue(any())).thenAnswer(invocation -> {
            MessageExtBrokerInner capturedMessage = invocation.getArgument(0);
            AckMsg ackMsg = JSON.parseObject(capturedMessage.getBody(), AckMsg.class);

            assertEquals(point.ackOffsetByIndex(msgIndex), ackMsg.getAckOffset());
            assertEquals(point.getStartOffset(), ackMsg.getStartOffset());
            assertEquals(point.getCId(), ackMsg.getConsumerGroup());
            assertEquals(point.getTopic(), ackMsg.getTopic());
            assertEquals(point.getQueueId(), ackMsg.getQueueId());
            assertEquals(point.getPopTime(), ackMsg.getPopTime());
            assertEquals(point.getBrokerName(), ackMsg.getBrokerName());

            PutMessageResult result = mock(PutMessageResult.class);
            when(result.getPutMessageStatus()).thenReturn(PutMessageStatus.PUT_OK);
            return result;
        });

        Method method = PopBufferMergeService.class.getDeclaredMethod("putAckToStore", PopBufferMergeService.PopCheckPointWrapper.class, byte.class, AtomicInteger.class);
        method.setAccessible(true);
        method.invoke(popBufferMergeService, pointWrapper, msgIndex, count);
        verify(escapeBridge, times(1)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class));
    }
}
