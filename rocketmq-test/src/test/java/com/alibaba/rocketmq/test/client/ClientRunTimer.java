package com.alibaba.rocketmq.test.client;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.alibaba.rocketmq.test.BaseTest;


public class ClientRunTimer extends BaseTest {

    @Test
    // 单机可以启动多个JVM，需要指定不同的实例名
    // 通过在代码里指定instanceName
    // 通过在Java启动参数中指定-Drocketmq.client.name=
    public void testMultiClient() throws MQClientException {
        DefaultMQPullConsumer consumer1 = new DefaultMQPullConsumer("example.consumer.active");
        consumer1.setInstanceName("DEFAULT1");
        consumer1.start();

        DefaultMQPullConsumer consumer2 = new DefaultMQPullConsumer("example.consumer.active");
        consumer2.setInstanceName("DEFAULT2");
        consumer2.start();

        System.setProperty("rocketmq.client.name", "DEFAULT3");
        DefaultMQPullConsumer consumer3 = new DefaultMQPullConsumer("example.consumer.active");
        consumer3.setInstanceName("DEFAULT3");
        consumer3.start();

        DefaultMQPullConsumer consumer4 = new DefaultMQPullConsumer("example.consumer.active4");
        consumer4.setInstanceName("DEFAULT1");
        consumer4.start();
        String clientid1 = MQClientManager.getInstance().getAndCreateMQClientFactory(consumer1).getClientId();
        String clientid2 = MQClientManager.getInstance().getAndCreateMQClientFactory(consumer2).getClientId();
        String clientid3 = MQClientManager.getInstance().getAndCreateMQClientFactory(consumer3).getClientId();
        String clientid4 = MQClientManager.getInstance().getAndCreateMQClientFactory(consumer4).getClientId();

        Assert.assertNotSame(clientid1, clientid2);
        Assert.assertNotSame(clientid1, clientid3);
        Assert.assertEquals(clientid1, clientid4);
        consumer1.shutdown();
        consumer2.shutdown();
        consumer3.shutdown();
        consumer4.shutdown();

    }


    @Test
    // 客户端提供四种寻址方式（发现Name Server地址的方式）
    // 指定通过在代码里指定Name Server地址
    public void testClientFindNameServer() throws MQClientException {

        // 指定通过在代码里指定Name Server地址
        DefaultMQPullConsumer consumer2 = new DefaultMQPullConsumer("example.consumer2.active");
        consumer2.setNamesrvAddr("127.0.0.2:9876");
        consumer2.setInstanceName("DEFAULT2");
        consumer2.start();
        Assert.assertEquals(consumer2.getNamesrvAddr(), "127.0.0.2:9876");

        // //通过设置环境变量NAMESRV_ADDR
        // DefaultMQPullConsumer consumer3=new
        // DefaultMQPullConsumer("example.consumer3.active");
        // consumer3.getClientConfig().setInstanceName("DEFAULT3");
        // consumer3.start();
        // Assert.assertEquals(consumer3.getClientConfig().getNamesrvAddr(),"10.235.136.47:9876;127.0.0.1:9876");

        // 由程序自动访问 http://diamondserver.tbsite.net:8080/rocketmq/nsaddr 获取Name
        // Server地址（推荐使用）
        DefaultMQPullConsumer consumer4 = new DefaultMQPullConsumer("example.consumer4.active");
        consumer4.setInstanceName("DEFAULT4");
        consumer4.start();
        TopAddressing topAddressing = new TopAddressing();
        String nameSrvAddr = topAddressing.fetchNSAddr();
        Assert.assertEquals(consumer4.getNamesrvAddr(), nameSrvAddr);

        // 通过在Java启动参数中指定-Drocketmq.namesrv.addr
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        DefaultMQPullConsumer consumer1 = new DefaultMQPullConsumer("example.consumer1.active");
        consumer1.setInstanceName("DEFAULT1");
        consumer1.start();
        Assert.assertEquals(consumer1.getNamesrvAddr(), "127.0.0.1:9876");

        consumer1.shutdown();
        consumer2.shutdown();
        // consumer3.shutdown();
        consumer4.shutdown();
    }
}
