/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * $Id: SendMessageTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.api;

import com.alibaba.rocketmq.broker.BrokerTestHarness;
import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.hook.SendMessageContext;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * @author zander
 */
public class SendMessageTest extends BrokerTestHarness{

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
        try {
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
            assertTrue(result.getSendStatus() == SendStatus.SEND_OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
