package com.alibaba.rocketmq.test.producer;

import java.io.File;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.test.BaseTest;


public class ProducerTest extends BaseTest {
    public static MQProducer producer;


    @BeforeClass
    @Override
    public void testInit() throws Exception {
        deleteDir(System.getProperty("user.home") + File.separator + "store");
        super.testInit();
        producer = new DefaultMQProducer("example.producer");
        producer.start();
        Thread.sleep(2000);
    }


    @Test
    public void testProducerMsg() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        SendResult sendResult = producer.send(msg);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }


    @Test
    public void testProducerOrderMsg() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        MessageQueueSelector selector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                Integer id = (Integer) arg;
                int index = id % mqs.size();
                return mqs.get(index);
            }
        };
        Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        SendResult sendResult = producer.send(msg, selector, 1);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }


    // @Test
    // public void testProducerOnlyOrderMsg() {
    // // TODO Auto-generated constructor stub
    // }
    @Test
    public void testProducerDelayMsg() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        msg.setDelayTimeLevel(4);
        SendResult sendResult = producer.send(msg);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }


    @Test
    public void testProducerMsgAndCreateTopic() throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException {
        Message msg =
                new Message("TopicTest1", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        SendResult sendResult = producer.send(msg);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }


    @Test
    public void testClientCreateTopicIsOrder() throws MQClientException {
        producer.createTopic("TopicTest", "newTopicTest", 1);
    }


    @Test
    public void testClientCreateTopicNoOrder() throws MQClientException {
        producer.createTopic("TopicTest", "newTopicTest", 1);
    }


    // @Test
    // 发送普通消息失败后自动重试下一个Broker，最多重试3次
    // public void TestClientCreateTopic() {
    // // TODO Auto-generated constructor stub
    // }
    @Test
    // 发送消息API支持三种通信方式
    public void testProducerSynMsg() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        SendResult sendResult = producer.send(msg);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }


    @Test
    // 发送消息API支持三种通信方式
    public void testProducerAsyMsg() throws MQClientException, RemotingException, InterruptedException {
        final SendCallback sendCallback = new SendCallback() {

            @Override
            public void onSuccess(SendResult sendResult) {
                Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
            }


            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
            }

        };
        Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        producer.send(msg, sendCallback);
    }


    @Test
    // 发送消息API支持三种通信方式
    public void testProducerOneWayMsg() throws MQClientException, RemotingException, InterruptedException {
        Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        producer.sendOneway(msg);
    }


    @AfterClass
    @Override
    public void testDown() throws Exception {
        producer.shutdown();
        Thread.sleep(2000);
        super.testDown();
        System.exit(0);
    }
}
