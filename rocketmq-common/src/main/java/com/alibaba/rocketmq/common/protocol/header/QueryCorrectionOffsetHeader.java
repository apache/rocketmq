/**
 * $Id: GetMinOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 查找被纠正的 offset
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-08-06
 */
public class QueryCorrectionOffsetHeader implements CommandCustomHeader {
    private String filterGroups;
    @CFNotNull
    private String compareGroup;
    @CFNotNull
    private String topic;


    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub
    }


    public String getFilterGroups() {
        return filterGroups;
    }


    public void setFilterGroups(String filterGroups) {
        this.filterGroups = filterGroups;
    }


    public String getCompareGroup() {
        return compareGroup;
    }


    public void setCompareGroup(String compareGroup) {
        this.compareGroup = compareGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }
}
