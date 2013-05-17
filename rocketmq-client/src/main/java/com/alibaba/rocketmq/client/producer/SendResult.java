/**
 * $Id: SendResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer;

import com.alibaba.rocketmq.common.MessageQueue;


/**
 * 发送消息结果
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class SendResult {
    private final SendStatus sendStatus;
    private final String msgId;
    private final MessageQueue messageQueue;
    private final long queueOffset;


    public SendResult(SendStatus sendStatus, String msgId, MessageQueue messageQueue, long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }


    public SendStatus getSendStatus() {
        return sendStatus;
    }


    public String getMsgId() {
        return msgId;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public long getQueueOffset() {
        return queueOffset;
    }


    @Override
    public String toString() {
        return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ", messageQueue=" + messageQueue
                + ", queueOffset=" + queueOffset + "]";
    }
}
