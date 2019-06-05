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

package org.apache.rocketmq.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.util.internal.StringUtil;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttSubscribeMessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.mqtt.client.IOTClientManagerImpl.IOT_GROUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class MqttSubscribeMessageHandlerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private DefaultMqttMessageProcessor defaultMqttMessageProcessor = new DefaultMqttMessageProcessor(new MqttConfig(), new SnodeConfig(), null, null, null);

    @Spy
    private MqttSubscribeMessageHandler mqttSubscribeMessageHandler = new MqttSubscribeMessageHandler(defaultMqttMessageProcessor);
    @Mock
    private RemotingChannel remotingChannel;

    @Test
    public void test_topicStartWithWildcard() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = MqttSubscribeMessageHandler.class.getDeclaredMethod("topicStartWithWildcard", List.class);
        method.setAccessible(true);

        List<MqttTopicSubscription> subscriptions1 = new ArrayList<>();
        subscriptions1.add(new MqttTopicSubscription("+/test", MqttQoS.AT_MOST_ONCE));
        boolean invoke1 = (boolean) method.invoke(mqttSubscribeMessageHandler, subscriptions1);
        assert invoke1;

        List<MqttTopicSubscription> subscriptions2 = new ArrayList<>();
        subscriptions2.add(new MqttTopicSubscription("test/topic", MqttQoS.AT_MOST_ONCE));
        boolean invoke2 = (boolean) method.invoke(mqttSubscribeMessageHandler, subscriptions2);
        assert !invoke2;

        List<MqttTopicSubscription> subscriptions3 = new ArrayList<>();
        subscriptions3.add(new MqttTopicSubscription("/test/topic", MqttQoS.AT_MOST_ONCE));
        boolean invoke3 = (boolean) method.invoke(mqttSubscribeMessageHandler, subscriptions3);
        assert invoke3;
    }

    @Test
    public void test_handleMessage_wrongMessageType() {
        MqttConnectMessage mqttConnectMessage = new MqttConnectMessage(new MqttFixedHeader(
            MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 200), new MqttConnectVariableHeader(null, 4, false, false, false, 0, false, false, 50), new MqttConnectPayload("abcd", "ttest", "message".getBytes(), "user", "password".getBytes()));

        exception.expect(WrongMessageTypeException.class);
        mqttSubscribeMessageHandler.handleMessage(mqttConnectMessage, remotingChannel);
    }

    @Test
    public void test_handleMessage_clientNotFound() {
        List<MqttTopicSubscription> subscriptions = new ArrayList<>();
        subscriptions.add(new MqttTopicSubscription("test/a", MqttQoS.AT_MOST_ONCE));
        MqttSubscribeMessage mqttSubscribeMessage = new MqttSubscribeMessage(new MqttFixedHeader(
            MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 200), MqttMessageIdVariableHeader.from(1), new MqttSubscribePayload(subscriptions));

        RemotingCommand remotingCommand = mqttSubscribeMessageHandler.handleMessage(mqttSubscribeMessage, remotingChannel);
        assertEquals(null, defaultMqttMessageProcessor.getIotClientManager().getClient(IOT_GROUP, remotingChannel));
        assert remotingCommand == null;
    }

    @Test
    public void test_handleMessage_emptyTopicFilter() {
        List<MqttTopicSubscription> subscriptions = new ArrayList<>();
        MqttSubscribeMessage mqttSubscribeMessage = new MqttSubscribeMessage(new MqttFixedHeader(
            MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 200), MqttMessageIdVariableHeader.from(1), new MqttSubscribePayload(subscriptions));

        MQTTSession mqttSession = Mockito.mock(MQTTSession.class);
        Mockito.when(mqttSession.getRemotingChannel()).thenReturn(remotingChannel);
//        Mockito.when(mqttSubscribeMessage.toString()).thenReturn("toString");
        defaultMqttMessageProcessor.getIotClientManager().register(IOT_GROUP, mqttSession);
        RemotingCommand remotingCommand = mqttSubscribeMessageHandler.handleMessage(mqttSubscribeMessage, remotingChannel);
        assertNotNull(defaultMqttMessageProcessor.getIotClientManager().getClient(IOT_GROUP, remotingChannel));
        assert remotingCommand == null;
    }

    @Test
    public void test_MqttSubscribePayload_toString() {
        List<MqttTopicSubscription> topicSubscriptions = new ArrayList<>();
        topicSubscriptions.add(new MqttTopicSubscription("test/topic", MqttQoS.AT_MOST_ONCE));

        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this)).append('[');
        for (int i = 0; i <= topicSubscriptions.size() - 1; i++) {
            builder.append(topicSubscriptions.get(i)).append(", ");
        }
        if (builder.substring(builder.length() - 2).equals(", ")) {
            builder.delete(builder.length() - 2, builder.length());
        }
        builder.append(']');
        System.out.println(builder.toString());
    }
}
