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

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionMQProducerWithTraceTest {

    @Spy
    private MQClientInstance mqClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mqClientAPIImpl;
    @Mock
    private EndTransactionHook endTransactionHook;

    private AsyncTraceDispatcher asyncTraceDispatcher;

    private TransactionMQProducer producer;
    private DefaultMQProducer traceProducer;

    private Message message;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";
    private String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
    private String producerGroupTraceTemp = TopicValidator.RMQ_SYS_TRACE_TOPIC + System.currentTimeMillis();
    private String customerTraceTopic = "rmq_trace_topic_12345";

    @Before
    public void init() throws Exception {
        TransactionListener transactionListener = new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        };
        producer = new TransactionMQProducer(null, producerGroupTemp, null, true, null);
        producer.setTransactionListener(transactionListener);

        producer.setNamesrvAddr("127.0.0.1:9876");
        message = new Message(topic, new byte[] {'a', 'b', 'c'});
        asyncTraceDispatcher = (AsyncTraceDispatcher) producer.getTraceDispatcher();
        traceProducer = asyncTraceDispatcher.getTraceProducer();

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mqClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mqClientFactory);

        Field fieldTrace = DefaultMQProducerImpl.class.getDeclaredField("mqClientFactory");
        fieldTrace.setAccessible(true);
        fieldTrace.set(traceProducer.getDefaultMQProducerImpl(), mqClientFactory);

        field = MQClientInstance.class.getDeclaredField("mqClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientFactory, mqClientAPIImpl);

        producer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        Field fieldHooks = DefaultMQProducerImpl.class.getDeclaredField("endTransactionHookList");
        fieldHooks.setAccessible(true);
        List<EndTransactionHook> hooks = new ArrayList<>();
        hooks.add(endTransactionHook);
        fieldHooks.set(producer.getDefaultMQProducerImpl(), hooks);

        when(mqClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mqClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));

    }

    @Test
    public void testSendMessageSync_WithTrace_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        traceProducer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTraceTemp, traceProducer.getDefaultMQProducerImpl());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final AtomicReference<EndTransactionContext> context = new AtomicReference<>();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                context.set((EndTransactionContext) mock.getArgument(0));
                return null;
            }

        }).when(endTransactionHook).endTransaction(any(EndTransactionContext.class));
        producer.sendMessageInTransaction(message, null);

        EndTransactionContext ctx = context.get();
        assertThat(ctx.getProducerGroup()).isEqualTo(producerGroupTemp);
        assertThat(ctx.getMsgId()).isEqualTo("123");
        assertThat(ctx.isFromTransactionCheck()).isFalse();
        assertThat(new String(ctx.getMessage().getBody())).isEqualTo(new String(message.getBody()));
        assertThat(ctx.getMessage().getTopic()).isEqualTo(topic);
    }

    @After
    public void terminate() {
        producer.shutdown();
    }

    public static TopicRouteData createTopicRoute() {
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
        sendResult.setOffsetMsgId(MessageDecoder.createMessageId(new InetSocketAddress("127.0.0.1", 12), 1));
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        sendResult.setMessageQueue(new MessageQueue(topic, "broker-trace", 0));
        return sendResult;
    }

}
