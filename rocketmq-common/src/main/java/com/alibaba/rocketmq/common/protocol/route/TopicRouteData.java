/**
 * $Id: TopicRouteData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.route;

import java.util.List;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Topic路由数据，从Name Server获取
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class TopicRouteData extends RemotingSerializable {
    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;


    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }


    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }


    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }


    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }


    public String getOrderTopicConf() {
        return orderTopicConf;
    }


    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }


    @Override
    public String toString() {
        return "TopicRouteData [queueDatas=" + queueDatas + ", brokerDatas=" + brokerDatas + ", orderTopicConf="
                + orderTopicConf + "]";
    }
}
