package com.alibaba.rocketmq.test.consumer;

import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.test.BaseTest;


public class PullConsumerTest extends BaseTest {
    public static DefaultMQPullConsumer consumer;


    @BeforeClass
    @Override
    public void testInit() throws Exception {
        super.testInit();
        consumer = new DefaultMQPullConsumer("example.consumer.active");
        MessageQueueListener listener = new MessageQueueListener() {

            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                System.out.println("Topic=" + topic);
                System.out.println("mqAll:" + mqAll);
                System.out.println("mqDivided:" + mqDivided);

            }

        };
        consumer.registerMessageQueueListener("TopicTest", listener);
        consumer.start();
        Thread.sleep(2000);
    }


    // @Test
    // public void testConsumerMsg() throws MQClientException,
    // RemotingException, MQBrokerException, InterruptedException{
    // List<MessageQueue> mqs =
    // consumer.fetchSubscribeMessageQueues("TopicTest");
    // for (MessageQueue mq : mqs) {
    // System.out.println("Consume from the queue: " + mq);
    // PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 0, 32);
    // System.out.println(pullResult);
    // switch (pullResult.getPullStatus()) {
    // case FOUND:
    // for(MessageExt mex:pullResult.getMsgFoundList()){
    // System.out.println(mex);
    // }
    // break;
    // case NO_MATCHED_MSG:
    // break;
    // case NO_NEW_MSG:
    // break;
    // case OFFSET_ILLEGAL:
    // break;
    // default:
    // break;
    // }
    // }
    //
    // }
    @Test
    public void testPull() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            PullResult pullResult = consumer.pull(mq, null, 0, 32);
            System.out.println(pullResult);
        }
    }


    @Test
    public void testPull2() throws MQClientException, RemotingException, InterruptedException {
        PullCallback pullCallback = new PullCallback() {

            @Override
            public void onSuccess(PullResult pullResult) {
                System.out.println(pullResult);

            }


            @Override
            public void onException(Throwable e) {
                e.printStackTrace();

            }

        };
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            consumer.pull(mq, null, 0, 32, pullCallback);

        }

    }


    @Test
    public void testPullBlockIfNotFound() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            // TODO 设置阻塞时间
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 0, 32);
            System.out.println(pullResult);
        }
    }


    @Test
    public void testPullBlockIfNotFound2() throws MQClientException, RemotingException, InterruptedException {
        PullCallback pullCallback = new PullCallback() {

            @Override
            public void onSuccess(PullResult pullResult) {
                System.out.println(pullResult);

            }


            @Override
            public void onException(Throwable e) {
                e.printStackTrace();

            }

        };
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            // TODO 设置阻塞时间
            consumer.pullBlockIfNotFound(mq, null, 0, 32, pullCallback);
        }
    }


    @Test
    public void testUpdateConsumeOffset() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 0, 32);
            consumer.updateConsumeOffset(mq, pullResult.getMaxOffset());
            System.out.println(pullResult.getMaxOffset());
        }
    }


    @Test
    public void testFetchConsumeOffset() throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            long offset = consumer.fetchConsumeOffset(mq, true);
            System.out.println(offset);
        }
    }


    @Test
    public void testFetchMessageQueuesInBalance() throws MQClientException {
        Set<MessageQueue> mqs = consumer.fetchMessageQueuesInBalance("TopicTest");
        for (MessageQueue mq : mqs) {
            System.out.println(mq);
        }
    }


    @Test
    public void testSendMessageBack() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            System.out.println("Consume from the queue: " + mq);
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 0, 32);
            System.out.println(pullResult);
            switch (pullResult.getPullStatus()) {
            case FOUND:
                for (MessageExt mex : pullResult.getMsgFoundList()) {
                    System.out.println(mex);
                    consumer.sendMessageBack(mex, 4);
                }
                break;
            case NO_MATCHED_MSG:
                break;
            case NO_NEW_MSG:
                break;
            case OFFSET_ILLEGAL:
                break;
            default:
                break;
            }
        }
    }


    @Test
    public void testFetchSubscribeMessageQueues() throws MQClientException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            System.out.println(mq);
        }
    }


    // @Test
    // 乱序方式订阅消息
    // @Test
    // 顺序方式订阅消息
    // @Test
    // 事务方式消费消息
    // @Test
    // 表达式过滤消息
    // @Test
    // 广播方式消费
    // @Test
    // 集群方式消费
    // @Test
    // 负载均衡实时性
    // @Test
    // 消息消费失败，发回Broker端，延时重试
    // @Test
    // 单队列并行消费
    // @Test
    // Consumer可自由选择从哪个消费进度开始消费
    // @Test
    // Consumer可自由选择从Master还是Slave消费
    // @Test
    // Pull方式自由拉消息
    // @Test
    // 订阅消息低延时，不堆积情况下可保证延时在10ms以内或左右（长轮询）

    @Test
    // 消息查询
    // 根据消息ID查询消息
    public void testSearcMsgbyId() throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            System.out.println("Consume from the queue: " + mq);
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 0, 32);
            System.out.println(pullResult);
            switch (pullResult.getPullStatus()) {
            case FOUND:
                for (MessageExt mex : pullResult.getMsgFoundList()) {
                    System.out.println(mex);
                    MessageExt data = consumer.viewMessage(mex.getMsgId());
                    // Assert.assertEquals(mex.toString(), data.toString());
                }
                break;
            case NO_MATCHED_MSG:
                break;
            case NO_NEW_MSG:
                break;
            case OFFSET_ILLEGAL:
                break;
            default:
                break;
            }
        }

    }


    @Test
    // 根据消息Key查询消息
    public void testSearcMsgbyKey() {
        // 单Tag消息服务端过滤

        // 多Tag消息服务端过滤

        // 多Tag消息任意tag服务端过滤

    }


    // 事务消息设置Key，可以通过Key来跟踪消息提交回滚状况

    @AfterClass
    @Override
    public void testDown() throws Exception {
        consumer.shutdown();
        Thread.sleep(2000);
        super.testDown();
        System.exit(0);
    }

}
