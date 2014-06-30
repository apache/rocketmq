package com.alibaba.rocketmq.common.protocol.body;

import java.util.Date;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * description
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-5-28
 */
public class QueueTimeSpan {
    private MessageQueue messageQueue;
    private long minTimeStamp;
    private long maxTimeStamp;
    private long consumeTimeStamp;


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public long getMinTimeStamp() {
        return minTimeStamp;
    }


    public void setMinTimeStamp(long minTimeStamp) {
        this.minTimeStamp = minTimeStamp;
    }


    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }


    public void setMaxTimeStamp(long maxTimeStamp) {
        this.maxTimeStamp = maxTimeStamp;
    }


    public long getConsumeTimeStamp() {
        return consumeTimeStamp;
    }


    public void setConsumeTimeStamp(long consumeTimeStamp) {
        this.consumeTimeStamp = consumeTimeStamp;
    }


    public String getMinTimeStampStr() {
        return UtilAll.formatDate(new Date(minTimeStamp), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
    }


    public String getMaxTimeStampStr() {
        return UtilAll.formatDate(new Date(maxTimeStamp), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
    }


    public String getConsumeTimeStampStr() {
        return UtilAll.formatDate(new Date(consumeTimeStamp), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
    }
}
