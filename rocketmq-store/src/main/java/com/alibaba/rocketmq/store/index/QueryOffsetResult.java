/**
 * $Id: QueryOffsetResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.index;

import java.util.List;


/**
 * 根据索引查询消息，返回结果
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class QueryOffsetResult {
    private final List<Long> phyOffsets;
    private final long indexLastUpdateTimestamp;
    private final long indexLastUpdatePhyoffset;


    public QueryOffsetResult(List<Long> phyOffsets, long indexLastUpdateTimestamp,
            long indexLastUpdatePhyoffset) {
        this.phyOffsets = phyOffsets;
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }


    public List<Long> getPhyOffsets() {
        return phyOffsets;
    }


    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }


    public long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }
}
