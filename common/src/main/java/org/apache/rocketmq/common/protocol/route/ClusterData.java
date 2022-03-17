package org.apache.rocketmq.common.protocol.route;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ClusterData {

    private String cluster;
    private String brokerName;

    public ClusterData(String cluster, String brokerName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ClusterData that = (ClusterData) o;

        return new EqualsBuilder().append(cluster, that.cluster).append(brokerName, that.brokerName).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(cluster).append(brokerName).toHashCode();
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
