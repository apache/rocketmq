/**
 * $Id: QueryMessageRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class QueryMessageRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String key;
    @CFNotNull
    private Integer maxNum;
    @CFNotNull
    private Long beginTimestamp;
    @CFNotNull
    private Long endTimestamp;


    @Override
    public void checkFields() throws RemotingCommandException {

    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getKey() {
        return key;
    }


    public void setKey(String key) {
        this.key = key;
    }


    public Integer getMaxNum() {
        return maxNum;
    }


    public void setMaxNum(Integer maxNum) {
        this.maxNum = maxNum;
    }


    public Long getBeginTimestamp() {
        return beginTimestamp;
    }


    public void setBeginTimestamp(Long beginTimestamp) {
        this.beginTimestamp = beginTimestamp;
    }


    public Long getEndTimestamp() {
        return endTimestamp;
    }


    public void setEndTimestamp(Long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }
}
