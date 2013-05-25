/**
 * $Id: PullConsumer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.example.simple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 主动拉消息方式的Consumer
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullConsumer {
    private static Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();


    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }


    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }


    public static void main(String[] args) {
        try {
            MQPullConsumer consumer = new DefaultMQPullConsumer("example.consumer.active");

            consumer.start();

            List<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
            for (MessageQueue mq : mqs) {
                System.out.println("Consume from the queue: " + mq);
                PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                System.out.println(pullResult);
                switch (pullResult.getPullStatus()) {
                case FOUND:
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

                putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
            }

            consumer.shutdown();
        }
        catch (MQClientException e) {
            e.printStackTrace();
        }
        catch (RemotingException e) {
            e.printStackTrace();
        }
        catch (MQBrokerException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
