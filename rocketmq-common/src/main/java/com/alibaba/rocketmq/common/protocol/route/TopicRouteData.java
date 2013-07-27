/**
 * $Id: TopicRouteData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.route;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;


/**
 * Topic路由数据，从Name Server获取
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
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
        return "TopicRouteData [queueDatas=" + queueDatas + ", brokerDatas=" + brokerDatas + ", orderTopicConf=" + orderTopicConf + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicRouteData other = (TopicRouteData) obj;
        if (brokerDatas == null) {
            if (other.brokerDatas != null)
                return false;
        } else if (!brokerDatas.equals(other.brokerDatas))
            return false;
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null)
                return false;
        } else if (!orderTopicConf.equals(other.orderTopicConf))
            return false;
        if (queueDatas == null) {
            if (other.queueDatas != null)
                return false;
        } else if (!queueDatas.equals(other.queueDatas))
            return false;
        return true;
    }
}
