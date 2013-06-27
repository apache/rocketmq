package com.alibaba.rocketmq.client.producer.selector;

import java.util.List;

import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 使用哈希算法来选择队列，顺序消息通常都这样做<br>
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-27
 */
public class SelectMessageQueueByHash implements MessageQueueSelector {

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = arg.hashCode();
        if (value < 0) {
            value = Math.abs(value);
        }

        value = value % mqs.size();
        return mqs.get(value);
    }
}
