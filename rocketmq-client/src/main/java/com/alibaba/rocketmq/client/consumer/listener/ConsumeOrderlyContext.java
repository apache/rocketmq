/**
 * $Id: ConsumeOrderlyContext.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.listener;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 消费消息上下文，同一队列的消息同一时刻只有一个线程消费，可保证同一队列消息顺序消费
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ConsumeOrderlyContext {
    /**
     * 要消费的消息属于哪个队列
     */
    private final MessageQueue messageQueue;
    /**
     * 消息Offset是否自动提交
     */
    private boolean autoCommit = true;
    /**
     * 将当前队列挂起时间，单位毫秒
     */
    private long suspendCurrentQueueTimeMillis = 1000;
    /**
     * 对于批量消费，ack至哪条消息，默认全部ack，至最后一条消息
     */
    private int ackIndex = Integer.MAX_VALUE;


    public ConsumeOrderlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public boolean isAutoCommit() {
        return autoCommit;
    }


    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }


    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }


    public int getAckIndex() {
        return ackIndex;
    }


    public void setAckIndex(int ackIndex) {
        this.ackIndex = ackIndex;
    }
}
