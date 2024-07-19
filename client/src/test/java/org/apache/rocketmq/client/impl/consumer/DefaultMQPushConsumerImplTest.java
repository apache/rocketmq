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

package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQPushConsumerImplTest {

    @Mock
    private DefaultMQPushConsumer defaultMQPushConsumer;

    @Mock
    private MQClientInstance mQClientFactory;

    @Mock
    private RebalanceImpl rebalanceImpl;

    @Mock
    private PullAPIWrapper pullAPIWrapper;

    @Mock
    private PullRequest pullRequest;

    @Mock
    private PopRequest popRequest;

    @Mock
    private ProcessQueue processQueue;

    @Mock
    private PopProcessQueue popProcessQueue;

    @Mock
    private MQClientAPIImpl mqClientAPIImpl;

    @Mock
    private OffsetStore offsetStore;

    private DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final String defaultKey = "defaultKey";

    private final String defaultTopic = "defaultTopic";

    private final String defaultBroker = "defaultBroker";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final String defaultGroup = "defaultGroup";

    private final long defaultTimeout = 3000L;

    @Test
    public void checkConfigTest() throws MQClientException {

        //test type
        thrown.expect(MQClientException.class);

        //test message
        thrown.expectMessage("consumeThreadMin (10) is larger than consumeThreadMax (9)");

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");

        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(9);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);

        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(consumer, null);
        defaultMQPushConsumerImpl.start();
    }

    @Test
    public void testHook() {
        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(defaultMQPushConsumer, null);
        defaultMQPushConsumerImpl.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "consumerHook";
            }

            @Override
            public void consumeMessageBefore(ConsumeMessageContext context) {
                assertThat(context).isNotNull();
            }

            @Override
            public void consumeMessageAfter(ConsumeMessageContext context) {
                assertThat(context).isNotNull();
            }
        });
        defaultMQPushConsumerImpl.registerFilterMessageHook(new FilterMessageHook() {
            @Override
            public String hookName() {
                return "filterHook";
            }

            @Override
            public void filterMessage(FilterMessageContext context) {
                assertThat(context).isNotNull();
            }
        });
        defaultMQPushConsumerImpl.executeHookBefore(new ConsumeMessageContext());
        defaultMQPushConsumerImpl.executeHookAfter(new ConsumeMessageContext());
    }

    @Ignore
    @Test
    public void testPush() throws Exception {
        when(defaultMQPushConsumer.getMessageListener()).thenReturn((MessageListenerConcurrently) (msgs, context) -> {
            assertThat(msgs).size().isGreaterThan(0);
            assertThat(context).isNotNull();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(defaultMQPushConsumer, null);
        try {
            defaultMQPushConsumerImpl.start();
        } finally {
            defaultMQPushConsumerImpl.shutdown();
        }
    }

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        MQAdminImpl mqAdminImpl = mock(MQAdminImpl.class);
        when(mQClientFactory.getMQAdminImpl()).thenReturn(mqAdminImpl);
        ConsumerStatsManager consumerStatsManager = mock(ConsumerStatsManager.class);
        ConsumeStatus consumeStatus = mock(ConsumeStatus.class);
        when(consumerStatsManager.consumeStatus(any(), any())).thenReturn(consumeStatus);
        when(mQClientFactory.getConsumerStatsManager()).thenReturn(consumerStatsManager);
        when(mQClientFactory.getPullMessageService()).thenReturn(mock(PullMessageService.class));
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPIImpl);
        FindBrokerResult findBrokerResult = mock(FindBrokerResult.class);
        when(findBrokerResult.getBrokerAddr()).thenReturn(defaultBrokerAddr);
        when(mQClientFactory.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn(findBrokerResult);
        Set<MessageQueue> messageQueueSet = Collections.singleton(createMessageQueue());
        ConcurrentMap<String, Set<MessageQueue>> topicMessageQueueMap = new ConcurrentHashMap<>();
        topicMessageQueueMap.put(defaultTopic, messageQueueSet);
        when(rebalanceImpl.getTopicSubscribeInfoTable()).thenReturn(topicMessageQueueMap);
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>();
        when(rebalanceImpl.getProcessQueueTable()).thenReturn(processQueueTable);
        RPCHook rpcHook = mock(RPCHook.class);
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(defaultMQPushConsumer, rpcHook);
        defaultMQPushConsumerImpl.setOffsetStore(offsetStore);
        FieldUtils.writeDeclaredField(defaultMQPushConsumerImpl, "mQClientFactory", mQClientFactory, true);
        FieldUtils.writeDeclaredField(defaultMQPushConsumerImpl, "rebalanceImpl", rebalanceImpl, true);
        FieldUtils.writeDeclaredField(defaultMQPushConsumerImpl, "pullAPIWrapper", pullAPIWrapper, true);
        FilterMessageHook filterMessageHook = mock(FilterMessageHook.class);
        ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
        filterMessageHookList.add(filterMessageHook);
        ConsumeMessageService consumeMessagePopService = mock(ConsumeMessageService.class);
        ConsumeMessageService consumeMessageService = mock(ConsumeMessageService.class);
        FieldUtils.writeDeclaredField(defaultMQPushConsumerImpl, "filterMessageHookList", filterMessageHookList, true);
        FieldUtils.writeDeclaredField(defaultMQPushConsumerImpl, "consumeMessageService", consumeMessageService, true);
        FieldUtils.writeDeclaredField(defaultMQPushConsumerImpl, "consumeMessagePopService", consumeMessagePopService, true);
        ConcurrentMap<String, SubscriptionData> subscriptionDataMap = new ConcurrentHashMap<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(defaultTopic);
        subscriptionDataMap.put(defaultTopic, subscriptionData);
        when(rebalanceImpl.getSubscriptionInner()).thenReturn(subscriptionDataMap);
    }

    @Test
    public void testFetchSubscribeMessageQueues() throws MQClientException {
        Set<MessageQueue> actual = defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(defaultTopic);
        assertNotNull(actual);
        Assert.assertEquals(1, actual.size());
        MessageQueue next = actual.iterator().next();
        assertEquals(defaultTopic, next.getTopic());
        assertEquals(defaultBroker, next.getBrokerName());
        assertEquals(0, next.getQueueId());
    }

    @Test
    public void testEarliestMsgStoreTime() throws MQClientException {
        assertEquals(0, defaultMQPushConsumerImpl.earliestMsgStoreTime(createMessageQueue()));
    }

    @Test
    public void testMaxOffset() throws MQClientException {
        assertEquals(0, defaultMQPushConsumerImpl.maxOffset(createMessageQueue()));
    }

    @Test
    public void testMinOffset() throws MQClientException {
        assertEquals(0, defaultMQPushConsumerImpl.minOffset(createMessageQueue()));
    }

    @Test
    public void testGetOffsetStore() {
        assertEquals(offsetStore, defaultMQPushConsumerImpl.getOffsetStore());
    }

    @Test
    public void testPullMessageWithStateNotOk() {
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithIsPause() {
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        defaultMQPushConsumerImpl.setPause(true);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithMsgCountFlowControl() {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        TreeMap<Long, MessageExt> treeMap = new TreeMap<>();
        treeMap.put(1L, new MessageExt());
        when(processQueue.getMsgTreeMap()).thenReturn(treeMap);
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(1);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithMsgSizeFlowControl() {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        TreeMap<Long, MessageExt> treeMap = new TreeMap<>();
        treeMap.put(1L, new MessageExt());
        when(processQueue.getMsgTreeMap()).thenReturn(treeMap);
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(1);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithMaxSpanFlowControl() {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMaxSpan()).thenReturn(2L);
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        TreeMap<Long, MessageExt> treeMap = new TreeMap<>();
        treeMap.put(1L, new MessageExt());
        when(processQueue.getMsgTreeMap()).thenReturn(treeMap);
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(10);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithNotLocked() {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        defaultMQPushConsumerImpl.setConsumeOrderly(true);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(10);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithSubscriptionDataIsNull() {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        when(pullRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(10);
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithNoMatchedMsg() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        when(pullRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(10);
        PullResult pullResultMock = mock(PullResult.class);
        when(pullAPIWrapper.processPullResult(any(MessageQueue.class), any(PullResult.class), any(SubscriptionData.class))).thenReturn(pullResultMock);
        when(pullResultMock.getPullStatus()).thenReturn(PullStatus.NO_MATCHED_MSG);
        doAnswer(invocation -> {
            PullCallback callback = invocation.getArgument(12);
            PullResult pullResult = mock(PullResult.class);
            callback.onSuccess(pullResult);
            return null;
        }).when(pullAPIWrapper).pullKernelImpl(
                any(MessageQueue.class),
                any(),
                any(),
                anyLong(),
                anyLong(),
                anyInt(),
                anyInt(),
                anyInt(),
                anyLong(),
                anyLong(),
                anyLong(),
                any(CommunicationMode.class),
                any(PullCallback.class));
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithOffsetIllegal() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        when(pullRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(10);
        PullResult pullResultMock = mock(PullResult.class);
        when(pullAPIWrapper.processPullResult(any(MessageQueue.class), any(PullResult.class), any(SubscriptionData.class))).thenReturn(pullResultMock);
        when(pullResultMock.getPullStatus()).thenReturn(PullStatus.OFFSET_ILLEGAL);
        doAnswer(invocation -> {
            PullCallback callback = invocation.getArgument(12);
            PullResult pullResult = mock(PullResult.class);
            callback.onSuccess(pullResult);
            return null;
        }).when(pullAPIWrapper).pullKernelImpl(
                any(MessageQueue.class),
                any(),
                any(),
                anyLong(),
                anyLong(),
                anyInt(),
                anyInt(),
                anyInt(),
                anyLong(),
                anyLong(),
                anyLong(),
                any(CommunicationMode.class),
                any(PullCallback.class));
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPullMessageWithException() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        when(processQueue.getMsgCount()).thenReturn(new AtomicLong(2));
        when(processQueue.getMsgSize()).thenReturn(new AtomicLong(3 * 1024 * 1024));
        when(pullRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(pullRequest.getProcessQueue()).thenReturn(processQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPullThresholdForQueue()).thenReturn(3);
        when(defaultMQPushConsumer.getPullThresholdSizeForQueue()).thenReturn(10);
        doAnswer(invocation -> {
            PullCallback callback = invocation.getArgument(12);
            callback.onException(new RuntimeException("exception"));
            return null;
        }).when(pullAPIWrapper).pullKernelImpl(
                any(MessageQueue.class),
                any(),
                any(),
                anyLong(),
                anyLong(),
                anyInt(),
                anyInt(),
                anyInt(),
                anyLong(),
                anyLong(),
                anyLong(),
                any(CommunicationMode.class),
                any(PullCallback.class));
        defaultMQPushConsumerImpl.pullMessage(pullRequest);
    }

    @Test
    public void testPopMessageWithFound() throws RemotingException, InterruptedException, MQClientException {
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        when(popRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(popRequest.getConsumerGroup()).thenReturn(defaultGroup);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        ConcurrentMap<String, SubscriptionData> subscriptionDataMap = new ConcurrentHashMap<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTagsSet(Collections.singleton("*"));
        subscriptionDataMap.put(defaultTopic, subscriptionData);
        when(rebalanceImpl.getSubscriptionInner()).thenReturn(subscriptionDataMap);
        doAnswer(invocation -> {
            PopCallback callback = invocation.getArgument(5);
            PopResult popResult = mock(PopResult.class);
            when(popResult.getPopStatus()).thenReturn(PopStatus.FOUND);
            when(popResult.getMsgFoundList()).thenReturn(Collections.singletonList(createMessageExt()));
            callback.onSuccess(popResult);
            return null;
        }).when(pullAPIWrapper).popAsync(
                any(MessageQueue.class),
                anyLong(),
                anyInt(),
                any(),
                anyLong(),
                any(PopCallback.class),
                anyBoolean(),
                anyInt(),
                anyBoolean(),
                any(),
                any());
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithException() throws RemotingException, InterruptedException, MQClientException {
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        when(popRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(popRequest.getConsumerGroup()).thenReturn(defaultGroup);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        ConcurrentMap<String, SubscriptionData> subscriptionDataMap = new ConcurrentHashMap<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTagsSet(Collections.singleton("*"));
        subscriptionDataMap.put(defaultTopic, subscriptionData);
        when(rebalanceImpl.getSubscriptionInner()).thenReturn(subscriptionDataMap);
        doAnswer(invocation -> {
            PopCallback callback = invocation.getArgument(5);
            callback.onException(new RuntimeException("exception"));
            return null;
        }).when(pullAPIWrapper).popAsync(
                any(MessageQueue.class),
                anyLong(),
                anyInt(),
                any(),
                anyLong(),
                any(PopCallback.class),
                anyBoolean(),
                anyInt(),
                anyBoolean(),
                any(),
                any());
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithNoNewMsg() throws RemotingException, InterruptedException, MQClientException {
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        when(popRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(popRequest.getConsumerGroup()).thenReturn(defaultGroup);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        ConcurrentMap<String, SubscriptionData> subscriptionDataMap = new ConcurrentHashMap<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTagsSet(Collections.singleton("*"));
        subscriptionDataMap.put(defaultTopic, subscriptionData);
        when(rebalanceImpl.getSubscriptionInner()).thenReturn(subscriptionDataMap);
        doAnswer(invocation -> {
            PopCallback callback = invocation.getArgument(5);
            PopResult popResult = mock(PopResult.class);
            when(popResult.getPopStatus()).thenReturn(PopStatus.NO_NEW_MSG);
            callback.onSuccess(popResult);
            return null;
        }).when(pullAPIWrapper).popAsync(
                any(MessageQueue.class),
                anyLong(),
                anyInt(),
                any(),
                anyLong(),
                any(PopCallback.class),
                anyBoolean(),
                anyInt(),
                anyBoolean(),
                any(),
                any());
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithPollingFull() throws RemotingException, InterruptedException, MQClientException {
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        when(popRequest.getMessageQueue()).thenReturn(createMessageQueue());
        when(popRequest.getConsumerGroup()).thenReturn(defaultGroup);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        ConcurrentMap<String, SubscriptionData> subscriptionDataMap = new ConcurrentHashMap<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTagsSet(Collections.singleton("*"));
        subscriptionDataMap.put(defaultTopic, subscriptionData);
        when(rebalanceImpl.getSubscriptionInner()).thenReturn(subscriptionDataMap);
        doAnswer(invocation -> {
            PopCallback callback = invocation.getArgument(5);
            PopResult popResult = mock(PopResult.class);
            when(popResult.getPopStatus()).thenReturn(PopStatus.POLLING_FULL);
            callback.onSuccess(popResult);
            return null;
        }).when(pullAPIWrapper).popAsync(any(
                        MessageQueue.class),
                anyLong(),
                anyInt(),
                any(),
                anyLong(),
                any(PopCallback.class),
                anyBoolean(),
                anyInt(),
                anyBoolean(),
                any(),
                any());
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithStateNotOk() {
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithIsPause() {
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        defaultMQPushConsumerImpl.setPause(true);
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithWaiAckMsgCountFlowControl() {
        when(popProcessQueue.getWaiAckMsgCount()).thenReturn(2);
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        when(defaultMQPushConsumer.getPopThresholdForQueue()).thenReturn(1);
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        defaultMQPushConsumerImpl.popMessage(popRequest);
    }

    @Test
    public void testPopMessageWithSubscriptionDataIsNull() throws RemotingException, InterruptedException, MQClientException {
        when(popProcessQueue.getWaiAckMsgCount()).thenReturn(2);
        when(popRequest.getPopProcessQueue()).thenReturn(popProcessQueue);
        when(popRequest.getMessageQueue()).thenReturn(createMessageQueue());
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        when(defaultMQPushConsumer.getPopThresholdForQueue()).thenReturn(3);
        defaultMQPushConsumerImpl.popMessage(popRequest);
        verify(pullAPIWrapper).popAsync(any(MessageQueue.class),
                eq(60000L),
                eq(0),
                any(),
                eq(15000L),
                any(PopCallback.class),
                eq(true),
                eq(0),
                eq(false),
                any(),
                any());
    }

    @Test
    public void testQueryMessage() throws InterruptedException, MQClientException {
        assertNull(defaultMQPushConsumerImpl.queryMessage(defaultTopic, defaultKey, 1, 0, 1));
    }

    @Test
    public void testQueryMessageByUniqKey() throws InterruptedException, MQClientException {
        assertNull(defaultMQPushConsumerImpl.queryMessageByUniqKey(defaultTopic, defaultKey));
    }

    @Test
    public void testSendMessageBack() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
        defaultMQPushConsumerImpl.sendMessageBack(createMessageExt(), 1, createMessageQueue());
        verify(mqClientAPIImpl).consumerSendMessageBack(
                eq(defaultBrokerAddr),
                any(),
                any(MessageExt.class),
                any(),
                eq(1),
                eq(5000L),
                eq(0));
    }

    @Test
    public void testAckAsync() throws MQBrokerException, RemotingException, InterruptedException {
        doAnswer(invocation -> {
            AckCallback callback = invocation.getArgument(2);
            AckResult result = mock(AckResult.class);
            when(result.getStatus()).thenReturn(AckStatus.OK);
            callback.onSuccess(result);
            return null;
        }).when(mqClientAPIImpl).ackMessageAsync(any(),
                anyLong(),
                any(AckCallback.class),
                any(AckMessageRequestHeader.class));
        defaultMQPushConsumerImpl.ackAsync(createMessageExt(), defaultGroup);
        verify(mqClientAPIImpl).ackMessageAsync(eq(defaultBrokerAddr),
                eq(3000L),
                any(AckCallback.class),
                any(AckMessageRequestHeader.class));
    }

    @Test
    public void testChangePopInvisibleTimeAsync() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        AckCallback callback = mock(AckCallback.class);
        String extraInfo = createMessageExt().getProperty(MessageConst.PROPERTY_POP_CK);
        defaultMQPushConsumerImpl.changePopInvisibleTimeAsync(defaultTopic, defaultGroup, extraInfo, defaultTimeout, callback);
        verify(mqClientAPIImpl).changeInvisibleTimeAsync(eq(defaultBroker),
                eq(defaultBrokerAddr),
                any(ChangeInvisibleTimeRequestHeader.class),
                eq(defaultTimeout),
                any(AckCallback.class));
    }

    @Test
    public void testShutdown() {
        defaultMQPushConsumerImpl.setServiceState(ServiceState.RUNNING);
        defaultMQPushConsumerImpl.shutdown();
        assertEquals(ServiceState.SHUTDOWN_ALREADY, defaultMQPushConsumerImpl.getServiceState());
    }

    @Test
    public void testSubscribe() throws MQClientException {
        defaultMQPushConsumerImpl.subscribe(defaultTopic, "fullClassname", "filterClassSource");
        RebalanceImpl actual = defaultMQPushConsumerImpl.getRebalanceImpl();
        assertEquals(1, actual.getSubscriptionInner().size());
    }

    @Test
    public void testSubscribeByMessageSelector() throws MQClientException {
        MessageSelector messageSelector = mock(MessageSelector.class);
        defaultMQPushConsumerImpl.subscribe(defaultTopic, messageSelector);
        RebalanceImpl actual = defaultMQPushConsumerImpl.getRebalanceImpl();
        assertEquals(1, actual.getSubscriptionInner().size());
    }

    @Test
    public void testSuspend() {
        defaultMQPushConsumerImpl.suspend();
        assertTrue(defaultMQPushConsumerImpl.isPause());
    }

    @Test
    public void testViewMessage() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
        assertNull(defaultMQPushConsumerImpl.viewMessage(defaultTopic, createMessageExt().getMsgId()));
    }

    @Test
    public void testResetOffsetByTimeStamp() throws MQClientException {
        ConcurrentMap<String, SubscriptionData> subscriptionDataMap = new ConcurrentHashMap<>();
        subscriptionDataMap.put(defaultTopic, new SubscriptionData());
        when(rebalanceImpl.getSubscriptionInner()).thenReturn(subscriptionDataMap);
        defaultMQPushConsumerImpl.resetOffsetByTimeStamp(System.currentTimeMillis());
        verify(mQClientFactory).resetOffset(eq(defaultTopic), any(), any());
    }

    @Test
    public void testSearchOffset() throws MQClientException {
        assertEquals(0, defaultMQPushConsumerImpl.searchOffset(createMessageQueue(), System.currentTimeMillis()));
    }

    @Test
    public void testQueryConsumeTimeSpan() throws InterruptedException, MQClientException, MQBrokerException, RemotingException {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.getBrokerDatas().add(createBrokerData());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(topicRouteData);
        List<QueueTimeSpan> actual = defaultMQPushConsumerImpl.queryConsumeTimeSpan(defaultTopic);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void testTryResetPopRetryTopic() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.getBrokerDatas().add(createBrokerData());
        MessageExt messageExt = createMessageExt();
        List<MessageExt> msgs = new ArrayList<>();
        messageExt.setTopic(MixAll.RETRY_GROUP_TOPIC_PREFIX + defaultGroup + "_" + defaultTopic);
        msgs.add(messageExt);
        defaultMQPushConsumerImpl.tryResetPopRetryTopic(msgs, defaultGroup);
        assertEquals(defaultTopic, msgs.get(0).getTopic());
    }

    @Test
    public void testGetPopDelayLevel() {
        int[] actual = defaultMQPushConsumerImpl.getPopDelayLevel();
        int[] expected = new int[]{10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testGetMessageQueueListener() {
        assertNull(defaultMQPushConsumerImpl.getMessageQueueListener());
    }

    @Test
    public void testConsumerRunningInfo() {
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueMap = new ConcurrentHashMap<>();
        ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueMap = new ConcurrentHashMap<>();
        processQueueMap.put(createMessageQueue(), new ProcessQueue());
        popProcessQueueMap.put(createMessageQueue(), new PopProcessQueue());
        when(rebalanceImpl.getProcessQueueTable()).thenReturn(processQueueMap);
        when(rebalanceImpl.getPopProcessQueueTable()).thenReturn(popProcessQueueMap);
        ConsumerRunningInfo actual = defaultMQPushConsumerImpl.consumerRunningInfo();
        assertNotNull(actual);
        assertEquals(1, actual.getSubscriptionSet().size());
        assertEquals(defaultTopic, actual.getSubscriptionSet().iterator().next().getTopic());
        assertEquals(1, actual.getMqTable().size());
        assertEquals(1, actual.getMqPopTable().size());
        assertEquals(1, actual.getStatusTable().size());
    }

    private BrokerData createBrokerData() {
        BrokerData result = new BrokerData();
        HashMap<Long, String> brokerAddrMap = new HashMap<>();
        brokerAddrMap.put(MixAll.MASTER_ID, defaultBrokerAddr);
        result.setBrokerAddrs(brokerAddrMap);
        result.setBrokerName(defaultBroker);
        return result;
    }

    private MessageQueue createMessageQueue() {
        MessageQueue result = new MessageQueue();
        result.setQueueId(0);
        result.setBrokerName(defaultBroker);
        result.setTopic(defaultTopic);
        return result;
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(defaultTopic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, defaultGroup);
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        long curTime = System.currentTimeMillis();
        result.setBornTimestamp(curTime - 1000);
        String popProps = String.format("%d %d %d %d %d %s %d %d %d", curTime, curTime, curTime, curTime, curTime, defaultBroker, 1, 0L, 1L);
        result.getProperties().put(MessageConst.PROPERTY_POP_CK, popProps);
        result.setKeys("keys");
        result.setTags("*");
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setBornHost(bornHost);
        result.setStoreHost(storeHost);
        return result;
    }
}
