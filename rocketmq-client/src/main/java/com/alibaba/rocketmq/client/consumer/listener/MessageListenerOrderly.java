/**
 * $Id: MessageListenerOrderly.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.listener;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 同一队列的消息同一时刻只能一个线程消费，可保证消息在同一队列严格有序消费
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public interface MessageListenerOrderly extends MessageListener {
    /**
     * 方法抛出异常等同于返回 ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
     * 
     * @param msgs
     *            msgs.size() >= 1
     * @param context
     * @return
     */
    public ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
            final ConsumeOrderlyContext context);
}
