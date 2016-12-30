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

/**
 * $Id: SendMessageTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.broker.api;

import org.apache.rocketmq.broker.BrokerTestHarness;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SendMessageTest extends BrokerTestHarness {

    MQClientAPIImpl client = new MQClientAPIImpl(new NettyClientConfig(), null, null, new ClientConfig());
    String topic = "UnitTestTopic";

    @Before
    @Override
    public void startup() throws Exception {
        super.startup();
        client.start();

    }

    @After
    @Override
    public void shutdown() throws Exception {
        client.shutdown();
        super.shutdown();
    }

    @Test
    public void testSendSingle() throws Exception {
        Message msg = new Message(topic, "TAG1 TAG2", "100200300", "body".getBytes());
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup("abc");
        requestHeader.setTopic(msg.getTopic());
        requestHeader.setDefaultTopic(MixAll.DEFAULT_TOPIC);
        requestHeader.setDefaultTopicQueueNums(4);
        requestHeader.setQueueId(0);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(msg.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

        SendResult result = client.sendMessage(brokerAddr, BROKER_NAME, msg, requestHeader, 1000 * 5,
            CommunicationMode.SYNC, new SendMessageContext(), null);
        assertEquals(result.getSendStatus(), SendStatus.SEND_OK);
    }
}
