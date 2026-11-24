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
package org.apache.rocketmq.broker.transaction.queue;

import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTransactionalMessageCheckListenerTest {

    private DefaultTransactionalMessageCheckListener listener;
    @Mock
    private MessageStore messageStore;
    @Mock
    private ProducerManager producerManager;
    @Mock
    private Broker2Client broker2Client;
    @Mock
    private Channel channel;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(),
        new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Before
    public void init() throws Exception {
        listener = new DefaultTransactionalMessageCheckListener();
        listener.setBrokerController(brokerController);
        brokerController.setMessageStore(messageStore);

    }

    @After
    public void destroy() {
//        brokerController.shutdown();
    }

    @Test
    public void testResolveHalfMsg() {
        listener.resolveHalfMsg(createMessageExt());
    }

    @Test
    public void testSendCheckMessage() throws Exception {
        MessageExt messageExt = createMessageExt();
        String producerGroup = "producerGroup";
        messageExt.setTopic(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
        when(brokerController.getProducerManager()).thenReturn(producerManager);
        when(producerManager.getAvailableChannel(producerGroup)).thenReturn(channel);
        when(brokerController.getBroker2Client()).thenReturn(broker2Client);

        listener.sendCheckMessage(messageExt);

        ArgumentCaptor<CheckTransactionStateRequestHeader> headerCaptor =
            ArgumentCaptor.forClass(CheckTransactionStateRequestHeader.class);
        verify(broker2Client).checkProducerTransactionState(
            eq(producerGroup), eq(channel), headerCaptor.capture(), eq(messageExt));
        assertThat(headerCaptor.getValue().getTopic()).isEqualTo("realTopic");
        assertThat(messageExt.getTopic()).isEqualTo("realTopic");
    }

    @Test
    public void sendCheckMessage() {
        listener.resolveDiscardMsg(createMessageExt());
    }

    private MessageExtBrokerInner createMessageExt() {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        MessageAccessor.putProperty(inner, MessageConst.PROPERTY_REAL_QUEUE_ID, "1");
        MessageAccessor.putProperty(inner, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "1234255");
        MessageAccessor.putProperty(inner, MessageConst.PROPERTY_REAL_TOPIC, "realTopic");
        inner.setTransactionId(inner.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        inner.setBody("check".getBytes());
        inner.setMsgId("12344567890");
        inner.setQueueId(0);
        return inner;
    }

    @Test
    public void testResolveDiscardMsg() {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        messageExt.setQueueId(0);
        messageExt.setBody("test resolve discard msg".getBytes());
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 54270));
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_TOPIC, "test_topic");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_PRODUCER_GROUP, "PID_TEST_DISCARD_MSG");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, "15");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "2");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TAGS, "test_discard_msg");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "AC14157E4F1C18B4AAC27EB1A0F30000");
        listener.resolveDiscardMsg(messageExt);
    }

}
