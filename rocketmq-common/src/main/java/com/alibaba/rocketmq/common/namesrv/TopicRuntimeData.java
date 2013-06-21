package com.alibaba.rocketmq.common.namesrv;

import static com.alibaba.rocketmq.common.protocol.route.ObjectConverter.topicRuntimeData2TopicRuntimeInfo;
import static com.alibaba.rocketmq.common.protocol.route.ObjectConverter.topicRuntimeInfo2TopicRuntimeData;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.rocketmq.common.protocol.MQProtos.TopicRuntimeInfo;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.ObjectConverter;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * 运行时收集的所有broker上topic相关的信息,只序列化nameserver端持久化的信息
 * 
 * @author lansheng.zj@taobao.com
 */
public class TopicRuntimeData {

    public static final String ORDER_PREFIX = "topic.num.";

    private transient Map<String/* topic */, List<QueueData>> topicBrokers;
    private Map<String/* topic */, String/* orderConf */> topicOrderConfs;
    private transient Map<String/* brokerName */, BrokerData> brokers;
    private List<String/* address */> brokerList;

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);


    // FIXME cluster information
    // private Map<String/*cluster name*/,?> clusterInfo;

    public TopicRuntimeData() {
        topicBrokers = new HashMap<String, List<QueueData>>();
        topicOrderConfs = new HashMap<String, String>();
        brokers = new HashMap<String, BrokerData>();
        brokerList = new ArrayList<String>();
    }


    public byte[] encode() {
        return topicRuntimeData2TopicRuntimeInfo(this).toByteArray();
    }


    public byte[] encodeSpecific() {
        TopicRuntimeData serial = new TopicRuntimeData();
        serial.setTopicOrderConfs(topicOrderConfs);
        serial.setBrokerList(brokerList);
        return topicRuntimeData2TopicRuntimeInfo(serial).toByteArray();
    }


    public static TopicRuntimeData decode(byte[] data) throws InvalidProtocolBufferException {
        TopicRuntimeInfo topicRuntimeInfo = TopicRuntimeInfo.parseFrom(data);
        return topicRuntimeInfo2TopicRuntimeData(topicRuntimeInfo);
    }


    public String encodeOrderTopicAsString() {
        StringBuilder encode = new StringBuilder();
        boolean isFirst = true;
        for (Entry<String, String> entry : topicOrderConfs.entrySet()) {
            if (!isFirst)
                encode.append("\n");

            encode.append(entry.getKey() + "=" + entry.getValue());
            isFirst = false;
        }

        return encode.toString();
    }


    public String encodeBrokerListAsString() {
        StringBuilder encode = new StringBuilder();
        boolean isFirst = true;
        for (String address : brokerList) {
            if (!isFirst)
                encode.append(";");

            encode.append(address);
            isFirst = false;
        }

        return encode.toString();
    }


    public String getOrderConfByTopic(String topic) {
        String conf = topicOrderConfs.get(ORDER_PREFIX + topic);
        if (null != conf && !"".equals(conf)) {
            return ORDER_PREFIX + topic + "=" + conf;
        }
        return "";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topicBrokers == null) ? 0 : topicBrokers.hashCode());
        result = prime * result + ((topicOrderConfs == null) ? 0 : topicOrderConfs.hashCode());
        result = prime * result + ((brokers == null) ? 0 : brokers.hashCode());
        result = prime * result + ((brokerList == null) ? 0 : brokerList.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {

        // TODO 测试比较的性能开销

        if (this == obj)
            return true;
        if (!(obj instanceof TopicRuntimeData))
            return false;

        TopicRuntimeData newTrd = (TopicRuntimeData) obj;
        boolean equal =
                ObjectConverter.equals(getTopicBrokers(), newTrd.getTopicBrokers())
                        && ObjectConverter.equals(getTopicOrderConfs(), newTrd.getTopicOrderConfs())
                        && ObjectConverter.equals(getBrokers(), newTrd.getBrokers())
                        && ObjectConverter.equals(getBrokerList(), newTrd.getBrokerList());

        return equal;
    }


    public void addPropertyChangeListener(final String propertyName, final PropertyChangeListener listener) {
        propertyChangeSupport.addPropertyChangeListener(propertyName, listener);
    }


    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        propertyChangeSupport.removePropertyChangeListener(listener);
    }


    public void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
        propertyChangeSupport.firePropertyChange(propertyName, oldValue, newValue);
    }


    public Map<String, List<QueueData>> getTopicBrokers() {
        return topicBrokers;
    }


    public void setTopicBrokers(Map<String, List<QueueData>> topicBrokers) {
        this.topicBrokers = topicBrokers;
    }


    public Map<String, String> getTopicOrderConfs() {
        return topicOrderConfs;
    }


    public void setTopicOrderConfs(Map<String, String> topicOrderConfs) {
        this.topicOrderConfs = topicOrderConfs;
    }


    public Map<String, BrokerData> getBrokers() {
        return brokers;
    }


    public void setBrokers(Map<String, BrokerData> brokers) {
        this.brokers = brokers;
    }


    public List<String> getBrokerList() {
        return brokerList;
    }


    public void setBrokerList(List<String> brokerList) {
        this.brokerList = brokerList;
    }
}
