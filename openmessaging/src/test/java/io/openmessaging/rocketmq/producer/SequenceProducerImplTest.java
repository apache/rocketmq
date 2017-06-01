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
package io.openmessaging.rocketmq.producer;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.MessagingAccessPointFactory;
import io.openmessaging.SequenceProducer;
import java.lang.reflect.Field;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SequenceProducerImplTest {

    private SequenceProducer producer;

    @Mock
    private DefaultMQProducer rocketmqProducer;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = MessagingAccessPointFactory
            .getMessagingAccessPoint("openmessaging:rocketmq://IP1:9876,IP2:9876/namespace");
        producer = messagingAccessPoint.createSequenceProducer();

        Field field = AbstractOMSProducer.class.getDeclaredField("rocketmqProducer");
        field.setAccessible(true);
        field.set(producer, rocketmqProducer);

        messagingAccessPoint.startup();
        producer.startup();
    }

    @Test
    public void testSend_WithCommit() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("TestMsgID");
        sendResult.setSendStatus(SendStatus.SEND_OK);
        when(rocketmqProducer.send(ArgumentMatchers.<Message>anyList())).thenReturn(sendResult);
        when(rocketmqProducer.getMaxMessageSize()).thenReturn(1024);
        final BytesMessage message = producer.createBytesMessageToTopic("HELLO_TOPIC", new byte[] {'a'});
        producer.send(message);
        producer.commit();
        assertThat(message.headers().getString(MessageHeader.MESSAGE_ID)).isEqualTo("TestMsgID");
    }

    @Test
    public void testRollback() {
        when(rocketmqProducer.getMaxMessageSize()).thenReturn(1024);
        final BytesMessage message = producer.createBytesMessageToTopic("HELLO_TOPIC", new byte[] {'a'});
        producer.send(message);
        producer.rollback();
        producer.commit(); //Commit nothing.
        assertThat(message.headers().getString(MessageHeader.MESSAGE_ID)).isEqualTo(null);
    }
}