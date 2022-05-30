package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;
import java.util.Map;

public class ProducerTableInfo extends RemotingSerializable {
    public ProducerTableInfo(Map<String, List<ProducerInfo>> data) {
        this.data = data;
    }

    private Map<String, List<ProducerInfo>> data;

    public Map<String, List<ProducerInfo>> getData() {
        return data;
    }

    public void setData(Map<String, List<ProducerInfo>> data) {
        this.data = data;
    }
}
