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

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.client.hook.SendMessageContext;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;


/**
 * @author shijia.wxr
 */
public class SendMessageTest {
    @Test
    public void test_sendMessage() throws Exception {
        BrokerController brokerController = new BrokerController(//
                new BrokerConfig(), //
                new NettyServerConfig(), //
                new NettyClientConfig(), //
                new MessageStoreConfig());
        boolean initResult = brokerController.initialize();
        System.out.println("initialize " + initResult);

        brokerController.start();

        MQClientAPIImpl client = new MQClientAPIImpl(new NettyClientConfig(), null, null, null);
        client.start();

        for (int i = 0; i < 100000; i++) {
            String topic = "UnitTestTopic_" + i % 3;
            Message msg = new Message(topic, "TAG1 TAG2", "100200300", ("Hello, Nice world\t" + i).getBytes());
            msg.setDelayTimeLevel(i % 3 + 1);

            try {
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup("abc");
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(MixAll.DEFAULT_TOPIC);
                requestHeader.setDefaultTopicQueueNums(4);
                requestHeader.setQueueId(i % 4);
                requestHeader.setSysFlag(0);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

                SendResult result = client.sendMessage("127.0.0.1:10911", "brokerName", msg, requestHeader, 1000 * 5,
                        CommunicationMode.SYNC, new SendMessageContext(), null);
                System.out.println(i + "\t" + result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        client.shutdown();

        brokerController.shutdown();
    }
}
