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

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.trace.hook.EndTransactionOpenTracingHookImpl;
import org.apache.rocketmq.client.trace.hook.SendMessageOpenTracingHookImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
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
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionMQProducerWithOpenTracingTest {

    @Spy
    private MQClientInstance mqClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mqClientAPIImpl;

    private TransactionMQProducer producer;

    private Message message;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";
    private String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
    private String producerGroupTraceTemp = TopicValidator.RMQ_SYS_TRACE_TOPIC + System.currentTimeMillis();
    private MockTracer tracer = new MockTracer();
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
        producer = new TransactionMQProducer(producerGroupTemp);
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageOpenTracingHookImpl(tracer));
        producer.getDefaultMQProducerImpl().registerEndTransactionHook(new EndTransactionOpenTracingHookImpl(tracer));
        producer.setTransactionListener(transactionListener);

        producer.setNamesrvAddr("127.0.0.1:9876");
        message = new Message(topic, new byte[] {'a', 'b', 'c'});

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mqClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mqClientFactory);

        field = MQClientInstance.class.getDeclaredField("mqClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientFactory, mqClientAPIImpl);

        producer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mqClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mqClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));

    }

    @Test
    public void testSendMessageSync_WithTrace_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        producer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTraceTemp, producer.getDefaultMQProducerImpl());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        producer.sendMessageInTransaction(message, null);

        assertThat(tracer.finishedSpans().size()).isEqualTo(2);
        MockSpan span = tracer.finishedSpans().get(1);
        assertThat(span.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey())).isEqualTo(topic);
        assertThat(span.tags().get(Tags.SPAN_KIND.getKey())).isEqualTo(Tags.SPAN_KIND_PRODUCER);
        assertThat(span.tags().get(TraceConstants.ROCKETMQ_MSG_ID)).isEqualTo("123");
        assertThat(span.tags().get(TraceConstants.ROCKETMQ_MSG_TYPE)).isEqualTo(MessageType.Trans_msg_Commit.name());
        assertThat(span.tags().get(TraceConstants.ROCKETMQ_TRANSACTION_STATE)).isEqualTo(LocalTransactionState.COMMIT_MESSAGE.name());
        assertThat(span.tags().get(TraceConstants.ROCKETMQ_IS_FROM_TRANSACTION_CHECK)).isEqualTo(false);
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
