/**
 * $Id: GetAllTopicConfigResponseHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class GetAllTopicConfigResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String version;
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private Long brokerId;
    @CFNotNull
    private String clusterName;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getVersion() {
        return version;
    }


    public void setVersion(String version) {
        this.version = version;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public String getClusterName() {
        return clusterName;
    }


    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }


    public Long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }
}
