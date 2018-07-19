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

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.producer.Producer;
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
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProducerImplTest {
    private Producer producer;

    @Mock
    private DefaultMQProducer rocketmqProducer;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = OMS
            .getMessagingAccessPoint("oms:rocketmq://IP1:9876,IP2:9876/namespace");
        producer = messagingAccessPoint.createProducer();

        Field field = AbstractOMSProducer.class.getDeclaredField("rocketmqProducer");
        field.setAccessible(true);
        field.set(producer, rocketmqProducer);

        messagingAccessPoint.startup();
        producer.startup();
    }

    @Test
    public void testSend_OK() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("TestMsgID");
        sendResult.setSendStatus(SendStatus.SEND_OK);
        when(rocketmqProducer.send(any(Message.class), anyLong())).thenReturn(sendResult);
        io.openmessaging.producer.SendResult omsResult =
            producer.send(producer.createBytesMessage("HELLO_TOPIC", new byte[] {'a'}));

        assertThat(omsResult.messageId()).isEqualTo("TestMsgID");
    }

    @Test
    public void testSend_Not_OK() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.FLUSH_DISK_TIMEOUT);

        when(rocketmqProducer.send(any(Message.class), anyLong())).thenReturn(sendResult);
        try {
            producer.send(producer.createBytesMessage("HELLO_TOPIC", new byte[] {'a'}));
            failBecauseExceptionWasNotThrown(OMSRuntimeException.class);
        } catch (Exception e) {
            assertThat(e).hasMessageContaining("Send message to RocketMQ broker failed.");
        }
    }

    @Test
    public void testSend_WithException() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        when(rocketmqProducer.send(any(Message.class), anyLong())).thenThrow(MQClientException.class);
        try {
            producer.send(producer.createBytesMessage("HELLO_TOPIC", new byte[] {'a'}));
            failBecauseExceptionWasNotThrown(OMSRuntimeException.class);
        } catch (Exception e) {
            assertThat(e).hasMessageContaining("Send message to RocketMQ broker failed.");
        }
    }

}