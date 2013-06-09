/**
 * $Id: SendResultExt.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.producer;

import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class SendResultExt extends SendResult {
    private final long tranStateTableOffset = 0;


    public SendResultExt(SendStatus sendStatus, String msgId, MessageQueue messageQueue, long queueOffset) {
        super(sendStatus, msgId, messageQueue, queueOffset);
    }
}
