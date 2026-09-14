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
package org.apache.rocketmq.broker.transaction.rocksdb;

import io.netty.channel.Channel;
import java.lang.reflect.Method;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.transaction.TransMessageRocksDBStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalMessageRocksDBServiceTest {
    @Mock
    private MessageStore messageStore;
    @Mock
    private TransMessageRocksDBStore transMessageRocksDBStore;
    @Mock
    private MessageRocksDBStorage messageRocksDBStorage;
    @Mock
    private BrokerController brokerController;
    @Mock
    private BrokerConfig brokerConfig;
    @Mock
    private ProducerManager producerManager;
    @Mock
    private Broker2Client broker2Client;
    @Mock
    private Channel channel;

    private TransactionalMessageRocksDBService service;

    @Before
    public void setUp() {
        when(messageStore.getTransMessageRocksDBStore()).thenReturn(transMessageRocksDBStore);
        when(transMessageRocksDBStore.getMessageRocksDBStorage()).thenReturn(messageRocksDBStorage);
        service = new TransactionalMessageRocksDBService(messageStore, brokerController);
    }

    @Test
    public void testSendCheckMessageUsesRealTopicInRequestHeader() throws Exception {
        String realTopic = "realTopic";
        String producerGroup = "producerGroup";
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(TopicValidator.RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_TOPIC, realTopic);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "1");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "messageId");
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getProducerManager()).thenReturn(producerManager);
        when(producerManager.getAvailableChannel(producerGroup)).thenReturn(channel);
        when(brokerController.getBroker2Client()).thenReturn(broker2Client);

        Method sendCheckMessage = TransactionalMessageRocksDBService.class
            .getDeclaredMethod("sendCheckMessage", MessageExt.class);
        sendCheckMessage.setAccessible(true);
        sendCheckMessage.invoke(service, messageExt);

        ArgumentCaptor<CheckTransactionStateRequestHeader> headerCaptor =
            ArgumentCaptor.forClass(CheckTransactionStateRequestHeader.class);
        verify(broker2Client).checkProducerTransactionState(
            eq(producerGroup), eq(channel), headerCaptor.capture(), eq(messageExt));
        assertThat(headerCaptor.getValue().getTopic()).isEqualTo(realTopic);
        assertThat(messageExt.getTopic()).isEqualTo(realTopic);
    }
}
