package org.apache.rocketmq.namesrv.controller.statemachine;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker id info, mapping from brokerAddress to brokerId.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:33
 */
public class BrokerIdInfo {
    private final String clusterName;
    private final String brokerName;
    // Start from 1
    private final AtomicLong brokerIdCount;
    private final HashMap<String/*Address*/, Long/*brokerId*/> brokerIdTable;

    public BrokerIdInfo(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerIdCount = new AtomicLong(1L);
        this.brokerIdTable = new HashMap<>();
    }

    public long newBrokerId() {
        return this.brokerIdCount.incrementAndGet();
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public HashMap<String, Long> getBrokerIdTable() {
        return brokerIdTable;
    }
}
