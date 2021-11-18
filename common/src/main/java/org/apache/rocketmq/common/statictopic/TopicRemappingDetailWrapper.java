package org.apache.rocketmq.common.statictopic;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

public class TopicRemappingDetailWrapper extends RemotingSerializable {
    public static final String TYPE_CREATE_OR_UPDATE = "CREATE_OR_UPDATE";
    public static final String TYPE_REMAPPING = "REMAPPING";


    private final String topic;
    private final String type;
    private final long epoch;
    private Map<Integer, String> expectedIdToBroker = new HashMap<Integer, String>();

    private Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<String, TopicConfigAndQueueMapping>();

    public TopicRemappingDetailWrapper(String topic, String type, long epoch, Map<Integer, String> expectedIdToBroker, Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
        this.topic = topic;
        this.type = type;
        this.epoch = epoch;
        this.expectedIdToBroker = expectedIdToBroker;
        this.brokerConfigMap = brokerConfigMap;
    }

    public String getTopic() {
        return topic;
    }

    public String getType() {
        return type;
    }

    public long getEpoch() {
        return epoch;
    }

    public Map<Integer, String> getExpectedIdToBroker() {
        return expectedIdToBroker;
    }

    public Map<String, TopicConfigAndQueueMapping> getBrokerConfigMap() {
        return brokerConfigMap;
    }
}
