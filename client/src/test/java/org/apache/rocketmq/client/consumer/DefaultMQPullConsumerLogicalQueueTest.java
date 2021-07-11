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
package org.apache.rocketmq.client.consumer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQRedirectException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQPullConsumerLogicalQueueTest {
    private MQClientInstance mQClientFactory;
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private DefaultMQPullConsumer pullConsumer;
    private String topic;
    private static final String cluster = "DefaultCluster";
    private static final String broker1Name = "BrokerA";
    private static final String broker1Addr = "127.0.0.2:10911";
    private static final String broker2Name = "BrokerB";
    private static final String broker2Addr = "127.0.0.3:10911";

    @Before
    public void init() throws Exception {
        topic = "FooBar" + System.nanoTime();

        mQClientFactory = spy(MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig()));

        FieldUtils.writeField(mQClientFactory, "mQClientAPIImpl", mQClientAPIImpl, true);

        pullConsumer = new DefaultMQPullConsumer("FooBarGroup" + System.nanoTime());
        pullConsumer.setNamesrvAddr("127.0.0.1:9876");
        pullConsumer.start();

        PullAPIWrapper pullAPIWrapper = pullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper();
        FieldUtils.writeDeclaredField(pullAPIWrapper, "mQClientFactory", mQClientFactory, true);

        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong(), anyBoolean(), ArgumentMatchers.<Set<Integer>>any())).thenReturn(createTopicRouteData());

        doReturn(new FindBrokerResult(broker1Addr, false)).when(mQClientFactory).findBrokerAddressInSubscribe(eq(broker1Name), anyLong(), anyBoolean());
        doReturn(new FindBrokerResult(broker2Addr, false)).when(mQClientFactory).findBrokerAddressInSubscribe(eq(broker2Name), anyLong(), anyBoolean());
    }

    @After
    public void terminate() {
        pullConsumer.shutdown();
    }

    @Test
    public void testStart_OffsetShouldNotNUllAfterStart() {
        Assert.assertNotNull(pullConsumer.getOffsetStore());
    }

    @Test
    public void testPullMessage_Success() throws Exception {
        doAnswer(new Answer<PullResultExt>() {
            @Override public PullResultExt answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                return DefaultMQPullConsumerLogicalQueueTest.this.createPullResult(requestHeader, PullStatus.FOUND, Collections.singletonList(new MessageExt()));
            }
        }).when(mQClientAPIImpl).pullMessage(eq(broker1Addr), any(PullMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC), (PullCallback) isNull());

        MessageQueue messageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, 0);
        PullResult pullResult = pullConsumer.pull(messageQueue, "*", 1024, 3);
        assertThat(pullResult).isNotNull();
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(1024 + 1);
        assertThat(pullResult.getMinOffset()).isEqualTo(123);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2048);
        assertThat(pullResult.getMsgFoundList()).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testPullMessage_NotFound() throws Exception {
        doAnswer(new Answer<PullResult>() {
            @Override public PullResult answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                return DefaultMQPullConsumerLogicalQueueTest.this.createPullResult(requestHeader, PullStatus.NO_NEW_MSG, new ArrayList<MessageExt>());
            }
        }).when(mQClientAPIImpl).pullMessage(eq(broker1Addr), any(PullMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC), (PullCallback) isNull());

        MessageQueue messageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, 0);
        PullResult pullResult = pullConsumer.pull(messageQueue, "*", 1024, 3);
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.NO_NEW_MSG);
    }

    @Test
    public void testPullMessageAsync_Success() throws Exception {
        doAnswer(new Answer<PullResult>() {
            @Override public PullResult answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                PullResult pullResult = DefaultMQPullConsumerLogicalQueueTest.this.createPullResult(requestHeader, PullStatus.FOUND, Collections.singletonList(new MessageExt()));

                PullCallback pullCallback = mock.getArgument(4);
                pullCallback.onSuccess(pullResult);
                return null;
            }
        }).when(mQClientAPIImpl).pullMessage(eq(broker1Addr), any(PullMessageRequestHeader.class), anyLong(), eq(CommunicationMode.ASYNC), any(PullCallback.class));

        final SettableFuture<PullResult> future = SettableFuture.create();
        MessageQueue messageQueue = new MessageQueue(topic, broker1Name, 0);
        pullConsumer.pull(messageQueue, "*", 1024, 3, new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                future.set(pullResult);
            }

            @Override
            public void onException(Throwable e) {
                future.setException(e);
            }
        });
        PullResult pullResult = future.get(3, TimeUnit.SECONDS);
        assertThat(pullResult).isNotNull();
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(1024 + 1);
        assertThat(pullResult.getMinOffset()).isEqualTo(123);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2048);
        assertThat(pullResult.getMsgFoundList()).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testPullMessageSync_Redirect() throws Exception {
        doAnswer(new Answer<PullResult>() {
            @Override public PullResult answer(InvocationOnMock mock) throws Throwable {
                throw new MQRedirectException(JSON.toJSONBytes(ImmutableList.of(
                    new LogicalQueueRouteData(0, 0, new MessageQueue(topic, broker1Name, 0), MessageQueueRouteState.Expired, 0, 0, 0, 0, broker1Addr),
                    new LogicalQueueRouteData(0, 10, new MessageQueue(topic, broker2Name, 0), MessageQueueRouteState.Normal, 0, -1, -1, -1, broker2Addr)
                )));
            }
        }).when(mQClientAPIImpl).pullMessage(eq(broker1Addr), any(PullMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC), (PullCallback) isNull());
        doAnswer(new Answer<PullResult>() {
            @Override public PullResult answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                return DefaultMQPullConsumerLogicalQueueTest.this.createPullResult(requestHeader, PullStatus.FOUND, Collections.singletonList(new MessageExt()));
            }
        }).when(mQClientAPIImpl).pullMessage(eq(broker2Addr), any(PullMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC), (PullCallback) isNull());

        MessageQueue messageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, 0);
        PullResult pullResult = pullConsumer.pull(messageQueue, "*", 1024, 3);
        assertThat(pullResult).isNotNull();
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(1024 + 1);
        assertThat(pullResult.getMinOffset()).isEqualTo(123 + 10);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2048 + 10);
        assertThat(pullResult.getMsgFoundList()).isEqualTo(Collections.emptyList());
    }

    private TopicRouteData createTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setBrokerDatas(ImmutableList.of(
            new BrokerData(cluster, broker1Name, new HashMap<Long, String>(Collections.singletonMap(MixAll.MASTER_ID, broker1Addr))),
            new BrokerData(cluster, broker2Name, new HashMap<Long, String>(Collections.singletonMap(MixAll.MASTER_ID, broker2Addr)))
        ));

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData;
        queueData = new QueueData();
        queueData.setBrokerName(broker1Name);
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        queueData = new QueueData();
        queueData.setBrokerName(broker2Name);
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);

        LogicalQueuesInfo info = new LogicalQueuesInfo();
        info.put(0, Lists.newArrayList(new LogicalQueueRouteData(0, 0, new MessageQueue(topic, broker1Name, 0), MessageQueueRouteState.Normal, 0, 0, 0, 0, broker1Addr)));
        topicRouteData.setLogicalQueuesInfo(info);
        return topicRouteData;
    }

    private PullResultExt createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
        List<MessageExt> messageExtList) throws Exception {
        return new PullResultExt(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList, 0, new byte[] {});
    }
}