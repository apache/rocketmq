package org.apache.rocketmq.common.statictopic;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopicRemappingDetailWrapper extends RemotingSerializable {
    public static final String TYPE_CREATE_OR_UPDATE = "CREATE_OR_UPDATE";
    public static final String TYPE_REMAPPING = "REMAPPING";

    public static final String SUFFIX_BEFORE = ".before";
    public static final String SUFFIX_AFTER = ".after";


    private final String topic;
    private final String type;
    private final long epoch;

    private Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<String, TopicConfigAndQueueMapping>();

    private Set<String> brokerToMapIn = new HashSet<String>();

    private Set<String> brokerToMapOut = new HashSet<String>();

    public TopicRemappingDetailWrapper(String topic, String type, long epoch, Map<String, TopicConfigAndQueueMapping> brokerConfigMap, Set<String> brokerToMapIn, Set<String> brokerToMapOut) {
        this.topic = topic;
        this.type = type;
        this.epoch = epoch;
        this.brokerConfigMap = brokerConfigMap;
        this.brokerToMapIn = brokerToMapIn;
        this.brokerToMapOut = brokerToMapOut;
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

    public Map<String, TopicConfigAndQueueMapping> getBrokerConfigMap() {
        return brokerConfigMap;
    }

    public Set<String> getBrokerToMapIn() {
        return brokerToMapIn;
    }

    public Set<String> getBrokerToMapOut() {
        return brokerToMapOut;
    }
}
