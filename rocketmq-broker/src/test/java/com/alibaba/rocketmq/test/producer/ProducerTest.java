package com.alibaba.rocketmq.test.producer;

import junit.framework.Assert;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.test.BaseTest;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProducerTest extends BaseTest{
	public static MQProducer producer;
	
	@BeforeClass 
	@Override
    public void testInit() throws Exception{
		super.testInit();
		producer = new DefaultMQProducer("example.producer");
		producer.start();
		Thread.sleep(2000);
	}
	@Test
	public void testProducerMsg() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		Message msg =
                new Message("TopicTest", "TagA", "TESTKEY", ("Hello RocketMQ from producerMsg").getBytes());
        SendResult sendResult = producer.send(msg);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
	}
	@Test
	public void testProducerOrderMsg() {
		// TODO Auto-generated constructor stub
	}
	@Test
	public void testProducerOnlyOrderMsg() {
		// TODO Auto-generated constructor stub
	}
	@Test
	public void testProducerDelayMsg() {
		// TODO Auto-generated constructor stub
	}
	@Test
	public void testProducerMsgAndCreateTopic() {
		// TODO Auto-generated constructor stub
	}
	@Test
	public void testClientCreateTopic() {
		// TODO Auto-generated constructor stub
	}
//	@Test
	//发送普通消息失败后自动重试下一个Broker，最多重试3次
//	public void TestClientCreateTopic() {
//		// TODO Auto-generated constructor stub
//	}
	@Test
//	发送消息API支持三种通信方式
	public void testProducerSynMsg() {
		// TODO Auto-generated constructor stub
	}
	@Test
//	发送消息API支持三种通信方式
	public void testProducerAsyMsg() {
		// TODO Auto-generated constructor stub
	}
	@Test
//	发送消息API支持三种通信方式
	public void testProducerOneWayMsg() {
		// TODO Auto-generated constructor stub
	}
	@AfterClass
	@Override
    public void testDown() throws Exception{  
		producer.shutdown();
		Thread.sleep(2000);
		super.testDown();
		System.exit(0);
    }  
}
