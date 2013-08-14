package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashMap;
import java.util.Set;

import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 协议中传输对象，内容为集群信息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-16
 */
public class ClusterInfo extends RemotingSerializable {
    private HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    private HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;


    public HashMap<String, BrokerData> getBrokerAddrTable() {
        return brokerAddrTable;
    }


    public void setBrokerAddrTable(HashMap<String, BrokerData> brokerAddrTable) {
        this.brokerAddrTable = brokerAddrTable;
    }


    public HashMap<String, Set<String>> getClusterAddrTable() {
        return clusterAddrTable;
    }


    public void setClusterAddrTable(HashMap<String, Set<String>> clusterAddrTable) {
        this.clusterAddrTable = clusterAddrTable;
    }
}
