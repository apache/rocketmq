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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.lang.reflect.Field;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullConsumerImplTest {
    private PullConsumer consumer;
    private String queueName = "HELLO_QUEUE";

    @Mock
    private DefaultMQPullConsumer rocketmqPullConsumer;
    private LocalMessageCache localMessageCache = null;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = OMS
            .getMessagingAccessPoint("oms:rocketmq://IP1:9876,IP2:9876/namespace");

        consumer = messagingAccessPoint.createPullConsumer(OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, "TestGroup"));
        consumer.attachQueue(queueName);

        Field field = PullConsumerImpl.class.getDeclaredField("rocketmqPullConsumer");
        field.setAccessible(true);
        field.set(consumer, rocketmqPullConsumer); //Replace

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setOperationTimeout(200);
        localMessageCache = spy(new LocalMessageCache(rocketmqPullConsumer, clientConfig));

        field = PullConsumerImpl.class.getDeclaredField("localMessageCache");
        field.setAccessible(true);
        field.set(consumer, localMessageCache);

        messagingAccessPoint.startup();
        consumer.startup();
    }

    @Test
    public void testPoll() {
        final byte[] testBody = new byte[] {'a', 'b'};
        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(testBody);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic(queueName);

        when(localMessageCache.poll()).thenReturn(consumedMsg);

        Message message = consumer.receive();
        assertThat(message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID)).isEqualTo("NewMsgId");
        assertThat(((BytesMessage) message).getBody(byte[].class)).isEqualTo(testBody);
    }

    @Test
    public void testPoll_WithTimeout() {
        //There is a default timeout value, @see ClientConfig#omsOperationTimeout.
        Message message = consumer.receive();
        assertThat(message).isNull();

        message = consumer.receive(OMS.newKeyValue().put(Message.BuiltinKeys.TIMEOUT, 100));
        assertThat(message).isNull();
    }
}