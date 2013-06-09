/**
 * $Id: PushConsumer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.example.simple;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 被动接收Broker Push消息的Consumer
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PushConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        MQPushConsumer consumer = new DefaultMQPushConsumer("example.consumer.passive");

        consumer.subscribe("TopicTest", "TagA || TagC || TagD");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            AtomicLong consumeTimes = new AtomicLong(0);


            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                System.out.println("Receive New Messages: " + msgs);
                // 模拟消费失败情况
                if ((this.consumeTimes.getAndIncrement() % 5) == 0) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");

        while (true) {
            Thread.sleep(1000);
        }
    }

}
