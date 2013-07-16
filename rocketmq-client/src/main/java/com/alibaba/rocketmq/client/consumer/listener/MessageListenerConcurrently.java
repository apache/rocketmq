/**
 * $Id: MessageListenerConcurrently.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.listener;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 同一队列的消息并行消费
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public interface MessageListenerConcurrently extends MessageListener {
    /**
     * 方法抛出异常等同于返回 ConsumeConcurrentlyStatus.RECONSUME_LATER
     * 
     * @param msgs
     * @param context
     * @return
     */
    public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
            final ConsumeConcurrentlyContext context);
}
