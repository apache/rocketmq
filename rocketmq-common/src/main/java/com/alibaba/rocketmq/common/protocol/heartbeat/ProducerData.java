/**
 * $Id: ProducerData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ProducerData {
    private String groupName;


    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }


    @Override
    public String toString() {
        return "ProducerData [groupName=" + groupName + "]";
    }
}
