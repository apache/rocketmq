/**
 * $Id: QueryMessageResponseHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class QueryMessageResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Long indexLastUpdateTimestamp;
    @CFNotNull
    private Long indexLastUpdatePhyoffset;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public Long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }


    public void setIndexLastUpdateTimestamp(Long indexLastUpdateTimestamp) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
    }


    public Long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }


    public void setIndexLastUpdatePhyoffset(Long indexLastUpdatePhyoffset) {
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }
}
