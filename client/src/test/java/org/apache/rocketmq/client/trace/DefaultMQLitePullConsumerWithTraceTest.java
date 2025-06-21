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
package org.apache.rocketmq.client.trace;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
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
public class DefaultMQLitePullConsumerWithTraceTest {

    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    @Mock
    private MQAdminImpl mQAdminImpl;

    private AsyncTraceDispatcher asyncTraceDispatcher;
    private DefaultMQProducer traceProducer;
    private RebalanceImpl rebalanceImpl;
    private OffsetStore offsetStore;
    private DefaultLitePullConsumerImpl litePullConsumerImpl;
    private String consumerGroup = "LitePullConsumerGroup";
    private String topic = "LitePullConsumerTest";
    private String brokerName = "BrokerA";
    private String producerGroupTraceTemp = TopicValidator.RMQ_SYS_TRACE_TOPIC + System.currentTimeMillis();

    private String customerTraceTopic = "rmq_trace_topic_12345";

    @BeforeClass
    public static void setUpEnv() {
        System.setProperty("rocketmq.client.logRoot", System.getProperty("java.io.tmpdir"));
    }

    @Before
    public void init() throws Exception {
        Field field = MQClientInstance.class.getDeclaredField("rebalanceService");
        field.setAccessible(true);
        RebalanceService rebalanceService = (RebalanceService) field.get(mQClientFactory);
        field = RebalanceService.class.getDeclaredField("waitInterval");
        field.setAccessible(true);
        field.set(rebalanceService, 100);
    }

    @Test
    public void testSubscribe_PollMessageSuccess_WithDefaultTraceTopic() throws Exception {
        DefaultLitePullConsumer litePullConsumer = createLitePullConsumerWithDefaultTraceTopic();
        try {
            Set<MessageQueue> messageQueueSet = new HashSet<>();
            messageQueueSet.add(createMessageQueue());
            litePullConsumerImpl.updateTopicSubscribeInfo(topic, messageQueueSet);
            litePullConsumer.setPollTimeoutMillis(20 * 1000);
            List<MessageExt> result = litePullConsumer.poll();
            assertThat(result.get(0).getTopic()).isEqualTo(topic);
            assertThat(result.get(0).getBody()).isEqualTo(new byte[] {'a'});
        } finally {
            litePullConsumer.shutdown();
        }
    }

    @Test
    public void testSubscribe_PollMessageSuccess_WithCustomizedTraceTopic() throws Exception {
        DefaultLitePullConsumer litePullConsumer = createLitePullConsumerWithCustomizedTraceTopic();
        try {
            Set<MessageQueue> messageQueueSet = new HashSet<>();
            messageQueueSet.add(createMessageQueue());
            litePullConsumerImpl.updateTopicSubscribeInfo(topic, messageQueueSet);
            litePullConsumer.setPollTimeoutMillis(20 * 1000);
            List<MessageExt> result = litePullConsumer.poll();
            assertThat(result.get(0).getTopic()).isEqualTo(topic);
            assertThat(result.get(0).getBody()).isEqualTo(new byte[] {'a'});
        } finally {
            litePullConsumer.shutdown();
        }
    }

    @Test
    public void testLitePullConsumerWithTraceTLS() throws Exception {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("consumerGroup");
        consumer.setUseTLS(true);
        consumer.setEnableMsgTrace(true);
        consumer.start();
        AsyncTraceDispatcher asyncTraceDispatcher = (AsyncTraceDispatcher) consumer.getTraceDispatcher();
        Assert.assertTrue(asyncTraceDispatcher.getTraceProducer().isUseTLS());
    }

    private DefaultLitePullConsumer createLitePullConsumerWithDefaultTraceTopic() throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(consumerGroup + System.currentTimeMillis());
        litePullConsumer.setEnableMsgTrace(true);
        litePullConsumer.setNamesrvAddr("127.0.0.1:9876");
        litePullConsumer.subscribe(topic, "*");
        suppressUpdateTopicRouteInfoFromNameServer(litePullConsumer);
        litePullConsumer.start();
        initDefaultLitePullConsumer(litePullConsumer);
        return litePullConsumer;
    }

    private DefaultLitePullConsumer createLitePullConsumerWithCustomizedTraceTopic() throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(consumerGroup + System.currentTimeMillis());
        litePullConsumer.setEnableMsgTrace(true);
        litePullConsumer.setCustomizedTraceTopic(customerTraceTopic);
        litePullConsumer.setNamesrvAddr("127.0.0.1:9876");
        litePullConsumer.subscribe(topic, "*");
        suppressUpdateTopicRouteInfoFromNameServer(litePullConsumer);
        litePullConsumer.start();
        initDefaultLitePullConsumer(litePullConsumer);
        return litePullConsumer;
    }

    private void initDefaultLitePullConsumer(DefaultLitePullConsumer litePullConsumer) throws Exception {
        asyncTraceDispatcher = (AsyncTraceDispatcher) litePullConsumer.getTraceDispatcher();
        traceProducer = asyncTraceDispatcher.getTraceProducer();
        Field field = DefaultLitePullConsumer.class.getDeclaredField("defaultLitePullConsumerImpl");
        field.setAccessible(true);
        litePullConsumerImpl = (DefaultLitePullConsumerImpl) field.get(litePullConsumer);
        field = DefaultLitePullConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(litePullConsumerImpl, mQClientFactory);

        PullAPIWrapper pullAPIWrapper = litePullConsumerImpl.getPullAPIWrapper();
        field = PullAPIWrapper.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pullAPIWrapper, mQClientFactory);

        Field fieldTrace = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        fieldTrace.setAccessible(true);
        fieldTrace.set(traceProducer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        field = MQClientInstance.class.getDeclaredField("mQAdminImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQAdminImpl);

        field = DefaultLitePullConsumerImpl.class.getDeclaredField("rebalanceImpl");
        field.setAccessible(true);
        rebalanceImpl = (RebalanceImpl) field.get(litePullConsumerImpl);
        field = RebalanceImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(rebalanceImpl, mQClientFactory);

        offsetStore = spy(litePullConsumerImpl.getOffsetStore());
        field = DefaultLitePullConsumerImpl.class.getDeclaredField("offsetStore");
        field.setAccessible(true);
        field.set(litePullConsumerImpl, offsetStore);

        traceProducer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTraceTemp, traceProducer.getDefaultMQProducerImpl());

        when(mQClientFactory.getMQClientAPIImpl().pullMessage(anyString(), any(PullMessageRequestHeader.class),
            anyLong(), any(CommunicationMode.class), nullable(PullCallback.class)))
            .thenAnswer(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock mock) throws Throwable {
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
                    return pullResult;
                }
            });

        when(mQClientFactory.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn(new FindBrokerResult("127.0.0.1:10911", false));

        doReturn(Collections.singletonList(mQClientFactory.getClientId())).when(mQClientFactory).findConsumerIdList(anyString(), anyString());

        doReturn(123L).when(offsetStore).readOffset(any(MessageQueue.class), any(ReadOffsetType.class));

    }

    private PullResultExt createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
        List<MessageExt> messageExtList) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (MessageExt messageExt : messageExtList) {
            outputStream.write(MessageDecoder.encode(messageExt, false));
        }
        return new PullResultExt(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList, 0, outputStream.toByteArray());
    }

    private MessageQueue createMessageQueue() {
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName(brokerName);
        messageQueue.setQueueId(0);
        messageQueue.setTopic(topic);
        return messageQueue;
    }

    private TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<>());
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

    private static void suppressUpdateTopicRouteInfoFromNameServer(DefaultLitePullConsumer litePullConsumer) throws IllegalAccessException {
        DefaultLitePullConsumerImpl defaultLitePullConsumerImpl = (DefaultLitePullConsumerImpl) FieldUtils.readDeclaredField(litePullConsumer, "defaultLitePullConsumerImpl", true);
        if (litePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
            litePullConsumer.changeInstanceNameToPIDWithGroupInfo(litePullConsumer.getConsumerGroup());
        }
        MQClientInstance mQClientFactory = spy(MQClientManager.getInstance().getOrCreateMQClientInstance(litePullConsumer, (RPCHook) FieldUtils.readDeclaredField(defaultLitePullConsumerImpl, "rpcHook", true)));
        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        factoryTable.put(litePullConsumer.buildMQClientId(), mQClientFactory);
        doReturn(false).when(mQClientFactory).updateTopicRouteInfoFromNameServer(anyString());
    }

}
