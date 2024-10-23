package org.apache.rocketmq.example.canary;

import org.apache.rocketmq.client.canary.CanaryAlgorithm;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByRandom;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * default canary algorithm based on SelectMessageQueueByRandom
 * used the first queue as the canary queue
 * @author xiejr
 * @since 2024/7/5 5:39 PM
 */
public class DefaultCanaryAlgorithmImpl extends SelectMessageQueueByRandom implements CanaryAlgorithm {


    @Override
    public boolean isCanary() {
        return Boolean.parseBoolean(System.getenv("canary"));
    }

    @Override
    public List<MessageQueue> getCanaryQueues(List<MessageQueue> queues) {
        ArrayList<MessageQueue> messageQueues = new ArrayList<>();
        messageQueues.add(queues.get(0));
        return messageQueues;
    }


    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        return super.select(getCanaryQueues(mqs), msg, arg);
    }
}
