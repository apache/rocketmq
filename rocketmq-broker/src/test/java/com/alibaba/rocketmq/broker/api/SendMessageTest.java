/**
 * $Id: SendMessageTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.api;

import org.junit.Test;

import com.alibaba.rocketmq.broker.BrokerController;
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


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
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

        MQClientAPIImpl client = new MQClientAPIImpl(new NettyClientConfig(), null);
        client.start();

        for (int i = 0; i < 100000; i++) {
            String topic = "UnitTestTopic_" + i % 3;
            Message msg =
                    new Message(topic, "TAG1 TAG2", "100200300", ("Hello, Nice world\t" + i).getBytes());
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

                SendResult result =
                        client.sendMessage("127.0.0.1:10911", "brokerName", msg, requestHeader, 1000 * 5,
                            CommunicationMode.SYNC, null);
                System.out.println(i + "\t" + result);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        client.shutdown();

        brokerController.shutdown();
    }
}
