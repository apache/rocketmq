/**
 * $Id: PullResultExt.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 只在内部使用，不对外公开
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullResultExt extends PullResult {
    private final boolean suggestPullingFromSlave;
    private byte[] messageBinary;


    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
            List<MessageExt> msgFoundList, final boolean suggestPullingFromSlave, final byte[] messageBinary) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestPullingFromSlave = suggestPullingFromSlave;
        this.messageBinary = messageBinary;
    }


    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }


    public byte[] getMessageBinary() {
        return messageBinary;
    }


    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }
}
