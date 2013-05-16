/**
 * $Id: ProducerData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

import com.alibaba.rocketmq.common.protocol.MQProtos.ProducerInfo;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ProducerData {
    private String groupName;


    public ProducerInfo encode() {
        ProducerInfo.Builder builder = ProducerInfo.newBuilder();
        builder.setGroupName(this.groupName);
        return builder.build();
    }


    public static ProducerData decode(ProducerInfo info) {
        ProducerData data = new ProducerData();
        data.setGroupName(info.getGroupName());
        return data;
    }


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
