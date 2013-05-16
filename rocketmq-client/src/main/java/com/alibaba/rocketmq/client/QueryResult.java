/**
 * $Id: QueryResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client;

import java.util.List;

import com.alibaba.rocketmq.common.MessageExt;


/**
 * 查询消息返回结果
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class QueryResult {
    private final long indexLastUpdateTimestamp;
    private final List<MessageExt> messageList;


    public QueryResult(long indexLastUpdateTimestamp, List<MessageExt> messageList) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
        this.messageList = messageList;
    }


    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }


    public List<MessageExt> getMessageList() {
        return messageList;
    }
}
