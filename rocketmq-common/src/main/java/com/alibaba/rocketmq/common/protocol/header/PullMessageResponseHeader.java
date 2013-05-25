/**
 * $Id: PullMessageResponseHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullMessageResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Boolean suggestPullingFromSlave;
    @CFNotNull
    private Long nextBeginOffset;
    @CFNotNull
    private Long minOffset;
    @CFNotNull
    private Long maxOffset;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public Boolean getSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }


    public void setSuggestPullingFromSlave(Boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }


    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }


    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }


    public Long getMinOffset() {
        return minOffset;
    }


    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }


    public Long getMaxOffset() {
        return maxOffset;
    }


    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }
}
