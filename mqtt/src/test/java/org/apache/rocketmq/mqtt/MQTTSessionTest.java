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

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.ClientRole;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.mqtt.client.InFlightMessage;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class MQTTSessionTest {

    private DefaultMqttMessageProcessor defaultMqttMessageProcessor = new DefaultMqttMessageProcessor(new MqttConfig(), new SnodeConfig(), null, null, null);
    private MQTTSession mqttSession = new MQTTSession("testClient", ClientRole.IOTCLIENT, new HashSet<String>() {
        {
            add("IOT_GROUP");
        }
    }, true, true, null, System.currentTimeMillis(), defaultMqttMessageProcessor);

    private ConcurrentHashMap<String, ConcurrentHashMap<String, TreeMap<Long, MessageExt>>> processTable = new ConcurrentHashMap<>();

    @Test
    public void test_put2processTable() throws Exception {
        Method method = MQTTSession.class.getDeclaredMethod("put2processTable", ConcurrentHashMap.class, String.class, String.class, MessageExt.class);
        for (int i = 0; i < 5; i++) {
            MessageExt messageExt = new MessageExt(0, System.currentTimeMillis(), null, System.currentTimeMillis(), null, null);
            messageExt.setQueueOffset(i);
            method.setAccessible(true);
            method.invoke(mqttSession, processTable, "broker1", "topic" + i, messageExt);
        }
        MessageExt messageExt_0 = new MessageExt(0, System.currentTimeMillis(), null, System.currentTimeMillis(), null, null);
        messageExt_0.setQueueOffset(10);
        method.invoke(mqttSession, processTable, "broker2", "topic0", messageExt_0);

        MessageExt messageExt_1 = new MessageExt(0, System.currentTimeMillis(), null, System.currentTimeMillis(), null, null);
        messageExt_1.setQueueOffset(11);
        method.invoke(mqttSession, processTable, "broker2", "topic0", messageExt_1);

        assertEquals(2, processTable.size());
        assertEquals(5, processTable.get("broker1").size());
        assertEquals(2, processTable.get("broker2").get("topic0@testClient").size());
    }

    @Test
    public void test_pushMessageQos1() {
        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setTopicName("topicTest");
        mqttHeader.setQosLevel(1);

        MessageExt messageExt = new MessageExt(0, System.currentTimeMillis(), null, System.currentTimeMillis(), null, null);
        messageExt.setTopic("topicTest");
        messageExt.setBody("Hello".getBytes());
        BrokerData brokerData = new BrokerData("DefaultCluster", "broker1", null);
        for (int i = 0; i < 10; i++) {
            mqttSession.pushMessageQos1(mqttHeader, messageExt, brokerData);
            assertEquals(i + 1, mqttHeader.getPacketId().intValue());
            System.out.println(mqttHeader.getPacketId());
        }
        assertEquals(0, mqttSession.getInflightSlots().get());
        assertEquals(10, mqttSession.getInflightWindow().size());
        assertEquals(10, mqttSession.getInflightTimeouts().size());

        mqttSession.pushMessageQos1(mqttHeader, messageExt, brokerData);

        assertEquals(0, mqttSession.getInflightSlots().get());
        assertEquals(10, mqttSession.getInflightWindow().size());
        assertEquals(10, mqttSession.getInflightTimeouts().size());
    }

    @Test
    public void test_pubAckReceived() {
        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setTopicName("topicTest");
        mqttHeader.setQosLevel(1);

        MessageExt messageExt = new MessageExt(0, System.currentTimeMillis(), null, System.currentTimeMillis(), null, null);
        messageExt.setTopic("topicTest");
        messageExt.setBody("Hello".getBytes());
        BrokerData brokerData = new BrokerData("DefaultCluster", "broker1", null);
        for (int i = 0; i < 2; i++) {
            mqttSession.pushMessageQos1(mqttHeader, messageExt, brokerData);
        }
        InFlightMessage inFlightMessage = mqttSession.pubAckReceived(1);
        assertEquals(9, mqttSession.getInflightSlots().intValue());
        assertEquals(1, mqttSession.getInflightWindow().size());
        assertEquals(false, mqttSession.getInUsePacketIds().containsKey(1));
    }
}
