/**
 * $Id: PullResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 拉消息返回结果
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PullResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private List<MessageExt> msgFoundList;


    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
            List<MessageExt> msgFoundList) {
        super();
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.msgFoundList = msgFoundList;
    }


    public PullStatus getPullStatus() {
        return pullStatus;
    }


    public long getNextBeginOffset() {
        return nextBeginOffset;
    }


    public long getMinOffset() {
        return minOffset;
    }


    public long getMaxOffset() {
        return maxOffset;
    }


    public List<MessageExt> getMsgFoundList() {
        return msgFoundList;
    }


    public void setMsgFoundList(List<MessageExt> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }


    @Override
    public String toString() {
        return "PullResult [pullStatus=" + pullStatus + ", nextBeginOffset=" + nextBeginOffset
                + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", msgFoundList="
                + (msgFoundList == null ? 0 : msgFoundList.size()) + "]";
    }
}
