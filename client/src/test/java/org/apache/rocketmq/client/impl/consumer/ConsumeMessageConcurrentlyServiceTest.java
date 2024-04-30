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

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumeMessageConcurrentlyServiceTest {
    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private PullAPIWrapper pullAPIWrapper;
    private RebalancePushImpl rebalancePushImpl;
    private DefaultMQPushConsumer pushConsumer;

    @Before
    public void init() throws Exception {
        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        Collection<MQClientInstance> instances = factoryTable.values();
        for (MQClientInstance instance : instances) {
            instance.shutdown();
        }
        factoryTable.clear();

        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.setPullInterval(60 * 1000);

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("rebalanceImpl");
        field.setAccessible(true);
        field.set(pushConsumerImpl, rebalancePushImpl);
        pushConsumer.subscribe(topic, "*");

        // suppress updateTopicRouteInfoFromNameServer
        pushConsumer.changeInstanceNameToPID();
        mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(pushConsumer, (RPCHook) FieldUtils.readDeclaredField(pushConsumerImpl, "rpcHook", true));
        mQClientFactory = spy(mQClientFactory);
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pushConsumerImpl, mQClientFactory);
        factoryTable.put(pushConsumer.buildMQClientId(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        pullAPIWrapper = spy(new PullAPIWrapper(mQClientFactory, consumerGroup, false));
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("pullAPIWrapper");
        field.setAccessible(true);
        field.set(pushConsumerImpl, pullAPIWrapper);

        pushConsumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().setmQClientFactory(mQClientFactory);

        when(mQClientFactory.getMQClientAPIImpl().pullMessage(anyString(), any(PullMessageRequestHeader.class),
                anyLong(), any(CommunicationMode.class), nullable(PullCallback.class)))
                .thenAnswer(new Answer<PullResult>() {
                    @Override
                    public PullResult answer(InvocationOnMock mock) throws Throwable {
                        PullMessageRequestHeader requestHeader = mock.getArgument(1);
                        MessageClientExt messageClientExt = new MessageClientExt();
                        messageClientExt.setTopic(topic);
                        messageClientExt.setQueueId(0);
                        messageClientExt.setMsgId("123");
                        messageClientExt.setBody(new byte[] {'a'});
                        messageClientExt.setOffsetMsgId("234");
                        messageClientExt.setBornHost(new InetSocketAddress(8080));
                        messageClientExt.setStoreHost(new InetSocketAddress(8080));
                        PullResult pullResult = createPullResult(requestHeader, PullStatus.FOUND, Collections.<MessageExt>singletonList(messageClientExt));
                        ((PullCallback) mock.getArgument(4)).onSuccess(pullResult);
                        return pullResult;
                    }
                });

        doReturn(new FindBrokerResult("127.0.0.1:10912", false)).when(mQClientFactory).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());
        doReturn(false).when(mQClientFactory).updateTopicRouteInfoFromNameServer(anyString());
        Set<MessageQueue> messageQueueSet = new HashSet<>();
        messageQueueSet.add(createPullRequest().getMessageQueue());
        pushConsumer.getDefaultMQPushConsumerImpl().updateTopicSubscribeInfo(topic, messageQueueSet);
        pushConsumer.start();
    }

    @Test
    public void testPullMessage_ConsumeSuccess() throws InterruptedException, RemotingException, MQBrokerException, NoSuchFieldException,Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<MessageExt> messageAtomic = new AtomicReference<>();

        ConsumeMessageConcurrentlyService  normalService = new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                messageAtomic.set(msgs.get(0));
                countDownLatch.countDown();
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(normalService);

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await();

        Thread.sleep(1000);

        ConsumeStatus stats = normalService.getConsumerStatsManager().consumeStatus(pushConsumer.getDefaultMQPushConsumerImpl().groupName(),topic);

        ConsumerStatsManager mgr  =   normalService.getConsumerStatsManager();

        Field statItmeSetField = mgr.getClass().getDeclaredField("topicAndGroupConsumeOKTPS");
        statItmeSetField.setAccessible(true);

        StatsItemSet itemSet = (StatsItemSet)statItmeSetField.get(mgr);
        StatsItem item = itemSet.getAndCreateStatsItem(topic + "@" + pushConsumer.getDefaultMQPushConsumerImpl().groupName());

        assertThat(item.getValue().sum()).isGreaterThan(0L);
        MessageExt msg = messageAtomic.get();
        assertThat(msg).isNotNull();
        assertThat(msg.getTopic()).isEqualTo(topic);
        assertThat(msg.getBody()).isEqualTo(new byte[] {'a'});
    }

    @After
    public void terminate() {
        pushConsumer.shutdown();
    }

    private PullRequest createPullRequest() {
        PullRequest pullRequest = new PullRequest();
        pullRequest.setConsumerGroup(consumerGroup);
        pullRequest.setNextOffset(1024);

        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName(brokerName);
        messageQueue.setQueueId(0);
        messageQueue.setTopic(topic);
        pullRequest.setMessageQueue(messageQueue);
        ProcessQueue processQueue = new ProcessQueue();
        processQueue.setLocked(true);
        processQueue.setLastLockTimestamp(System.currentTimeMillis());
        pullRequest.setProcessQueue(processQueue);

        return pullRequest;
    }

    private PullResultExt createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
                                           List<MessageExt> messageExtList) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (MessageExt messageExt : messageExtList) {
            outputStream.write(MessageDecoder.encode(messageExt, false));
        }
        return new PullResultExt(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList, 0, outputStream.toByteArray());
    }

    @Test
    public void testConsumeThreadName() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<String> consumeThreadName = new AtomicReference<>();

        StringBuilder consumeGroup2 = new StringBuilder();
        for (int i = 0; i < 101; i++) {
            consumeGroup2.append(i).append("#");
        }
        pushConsumer.setConsumerGroup(consumeGroup2.toString());
        ConsumeMessageConcurrentlyService  normalService = new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                consumeThreadName.set(Thread.currentThread().getName());
                countDownLatch.countDown();
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(normalService);

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await();
        if (consumeGroup2.length() <= 100) {
            assertThat(consumeThreadName.get()).startsWith("ConsumeMessageThread_" + consumeGroup2 + "_");
        } else {
            assertThat(consumeThreadName.get()).startsWith("ConsumeMessageThread_" + consumeGroup2.substring(0, 100) + "_");
        }
    }
}
