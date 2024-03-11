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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractDefaultMQProducerTest {

    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private DefaultMQProducer producer;

    private Message message;
    String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";
    private String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
    private String producerGroupTraceTemp = TopicValidator.RMQ_SYS_TRACE_TOPIC + System.currentTimeMillis();

    abstract SendMessageHook hook();

    abstract void assertResults();

    @Before
    public void init() throws Exception {

        producer = new DefaultMQProducer(producerGroupTemp);
        producer.getDefaultMQProducerImpl().registerSendMessageHook(hook());
        producer.setNamesrvAddr("127.0.0.1:9876");
        message = new Message(topic, new byte[] {'a', 'b', 'c'});

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));

    }

    @Test
    public void testSendMessageSync_WithTrace_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        producer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTraceTemp, producer.getDefaultMQProducerImpl());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        producer.send(message);
        assertResults();
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
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

}
