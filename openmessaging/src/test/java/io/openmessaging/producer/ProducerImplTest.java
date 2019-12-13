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
package io.openmessaging.producer;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.Producer;
import java.lang.reflect.Field;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.impl.rocketmq.ProducerImpl;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProducerImplTest {

    private Producer producer;

    private final String topic = "TopicTest";

    @Mock
    private DefaultMQProducer rocketmqProducer;

    @Mock
    private DefaultMQProducerImpl defaultMQProducerImpl;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = OMS
            .getMessagingAccessPoint("oms:rocketmq://127.0.0.1:9876/namespace");
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.GROUP_ID, "GID-producer-test");
        properties.setProperty(PropertyKeyConst.NAMESRV_ADDR, "127.0.0.1:9876");
        producer = messagingAccessPoint.createProducer(properties);
        Field field = ProducerImpl.class.getDeclaredField("defaultMQProducer");
        field.setAccessible(true);
        field.set(producer, rocketmqProducer);
        when(rocketmqProducer.getDefaultMQProducerImpl()).thenReturn(defaultMQProducerImpl);
        when(defaultMQProducerImpl.getServiceState()).thenReturn(ServiceState.RUNNING);
        producer.start();
    }

    @Test
    public void testSend_OK() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult sendResult = createSendResult(SendStatus.SEND_OK);
        when(rocketmqProducer.send(any(org.apache.rocketmq.common.message.Message.class))).thenReturn(sendResult);
        io.openmessaging.api.SendResult omsResult = producer.send(createMessage());

        assertThat(omsResult.getMessageId()).isEqualTo("TestMsgID");
    }

    @Test
    public void testSend_Not_OK() {
        try {
            producer.send(createMessage());
            failBecauseExceptionWasNotThrown(ONSClientException.class);
        } catch (Exception e) {
            assertThat(e).hasMessageContaining("send exception");
        }
    }

    private Message createMessage() {
        Message message = new Message();
        message.setTopic(topic);
        message.setBody("OpenMessaging Test".getBytes());
        return message;
    }

    private SendResult createSendResult(SendStatus status) {
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setTopic(topic);

        SendResult sendResult = new SendResult();
        sendResult.setMsgId("TestMsgID");
        sendResult.setMessageQueue(messageQueue);
        sendResult.setSendStatus(status);
        return sendResult;
    }

}