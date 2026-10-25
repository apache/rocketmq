/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.tools.command.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ClusterHealthReport {
    public enum Status {
        HEALTHY,
        UNHEALTHY
    }

    public enum NameServerStatus {
        HEALTHY,
        UNHEALTHY,
        SKIPPED
    }

    private long timestamp;
    private long durationMillis;
    private String target;
    private Status status;
    private NameServerStatus nameServerStatus;
    private String nameServerDetail;
    private int totalBrokers;
    private int healthyBrokers;
    private int unhealthyBrokers;
    private List<BrokerHealthResult> brokers = new ArrayList<>();

    public void complete() {
        List<BrokerHealthResult> safeBrokers = brokers == null ? new ArrayList<>() : brokers;
        safeBrokers.sort(Comparator.comparing(BrokerHealthResult::getClusterName)
            .thenComparing(BrokerHealthResult::getBrokerName)
            .thenComparingLong(BrokerHealthResult::getBrokerId)
            .thenComparing(BrokerHealthResult::getBrokerAddr));
        brokers = safeBrokers;
        totalBrokers = brokers.size();
        healthyBrokers = 0;
        unhealthyBrokers = 0;
        for (BrokerHealthResult broker : brokers) {
            if (BrokerHealthResult.Status.HEALTHY.equals(broker.getStatus())) {
                healthyBrokers++;
            } else {
                unhealthyBrokers++;
            }
        }

        boolean explicitlyUnhealthy = Status.UNHEALTHY.equals(status);
        boolean nameServerHealthy = NameServerStatus.HEALTHY.equals(nameServerStatus)
            || NameServerStatus.SKIPPED.equals(nameServerStatus);
        status = !explicitlyUnhealthy && nameServerHealthy && unhealthyBrokers == 0
            ? Status.HEALTHY : Status.UNHEALTHY;
    }

    public void markNoBrokers(String detail) {
        status = Status.UNHEALTHY;
        nameServerDetail = detail;
    }

    public boolean isHealthy() {
        return Status.HEALTHY.equals(status);
    }

    public String toJson() {
        return RemotingSerializable.toJson(this, true);
    }

    public String toText() {
        StringBuilder builder = new StringBuilder();
        builder.append("STATUS       ").append(status).append(System.lineSeparator());
        builder.append("TARGET       ").append(valueOrDash(target)).append(System.lineSeparator());
        builder.append("NAMESERVER   ").append(nameServerStatus);
        if (hasText(nameServerDetail)) {
            builder.append(" (").append(singleLine(nameServerDetail)).append(')');
        }
        builder.append(System.lineSeparator());
        builder.append("SUMMARY      ")
            .append(healthyBrokers).append(" healthy, ")
            .append(unhealthyBrokers).append(" unhealthy, ")
            .append(totalBrokers).append(" total, ")
            .append(durationMillis).append(" ms")
            .append(System.lineSeparator());

        if (!brokers.isEmpty()) {
            builder.append(System.lineSeparator());
            builder.append(String.format("%-16s %-24s %-5s %-22s %-10s %-8s %-16s %s%n",
                "#Cluster", "#Broker", "#BID", "#Address", "#Status", "#RT(ms)", "#Version", "#Detail"));
            for (BrokerHealthResult broker : brokers) {
                builder.append(String.format("%-16s %-24s %-5d %-22s %-10s %-8d %-16s %s%n",
                    valueOrDash(broker.getClusterName()),
                    valueOrDash(broker.getBrokerName()),
                    broker.getBrokerId(),
                    valueOrDash(broker.getBrokerAddr()),
                    broker.getStatus(),
                    broker.getLatencyMillis(),
                    valueOrDash(broker.getBrokerVersion()),
                    singleLine(broker.getDetail())));
            }
        }
        return builder.toString();
    }

    private static String valueOrDash(String value) {
        return hasText(value) ? value : "-";
    }

    private static String singleLine(String value) {
        if (value == null) {
            return "-";
        }
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    private static boolean hasText(String value) {
        return value != null && !value.isEmpty();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public void setDurationMillis(long durationMillis) {
        this.durationMillis = durationMillis;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public NameServerStatus getNameServerStatus() {
        return nameServerStatus;
    }

    public void setNameServerStatus(NameServerStatus nameServerStatus) {
        this.nameServerStatus = nameServerStatus;
    }

    public String getNameServerDetail() {
        return nameServerDetail;
    }

    public void setNameServerDetail(String nameServerDetail) {
        this.nameServerDetail = nameServerDetail;
    }

    public int getTotalBrokers() {
        return totalBrokers;
    }

    public void setTotalBrokers(int totalBrokers) {
        this.totalBrokers = totalBrokers;
    }

    public int getHealthyBrokers() {
        return healthyBrokers;
    }

    public void setHealthyBrokers(int healthyBrokers) {
        this.healthyBrokers = healthyBrokers;
    }

    public int getUnhealthyBrokers() {
        return unhealthyBrokers;
    }

    public void setUnhealthyBrokers(int unhealthyBrokers) {
        this.unhealthyBrokers = unhealthyBrokers;
    }

    public List<BrokerHealthResult> getBrokers() {
        return Collections.unmodifiableList(brokers);
    }

    public void setBrokers(List<BrokerHealthResult> brokers) {
        this.brokers = brokers == null ? new ArrayList<>() : new ArrayList<>(brokers);
    }
}
