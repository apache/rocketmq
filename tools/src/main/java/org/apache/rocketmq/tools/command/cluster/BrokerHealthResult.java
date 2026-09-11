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

public class BrokerHealthResult {
    public enum Status {
        HEALTHY,
        UNHEALTHY
    }

    private String clusterName;
    private String brokerName;
    private long brokerId;
    private String brokerAddr;
    private Status status;
    private String detail;
    private long latencyMillis;
    private String brokerVersion;
    private Boolean brokerActive;

    public static BrokerHealthResult healthy(BrokerTarget target, long latencyMillis,
        String brokerVersion, Boolean brokerActive) {
        BrokerHealthResult result = fromTarget(target);
        result.setStatus(Status.HEALTHY);
        result.setDetail("Broker runtime RPC succeeded");
        result.setLatencyMillis(latencyMillis);
        result.setBrokerVersion(brokerVersion);
        result.setBrokerActive(brokerActive);
        return result;
    }

    public static BrokerHealthResult unhealthy(BrokerTarget target, long latencyMillis, String detail) {
        BrokerHealthResult result = fromTarget(target);
        result.setStatus(Status.UNHEALTHY);
        result.setDetail(detail);
        result.setLatencyMillis(latencyMillis);
        return result;
    }

    private static BrokerHealthResult fromTarget(BrokerTarget target) {
        BrokerHealthResult result = new BrokerHealthResult();
        result.setClusterName(target.getClusterName());
        result.setBrokerName(target.getBrokerName());
        result.setBrokerId(target.getBrokerId());
        result.setBrokerAddr(target.getBrokerAddr());
        return result;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public long getLatencyMillis() {
        return latencyMillis;
    }

    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    public String getBrokerVersion() {
        return brokerVersion;
    }

    public void setBrokerVersion(String brokerVersion) {
        this.brokerVersion = brokerVersion;
    }

    public Boolean getBrokerActive() {
        return brokerActive;
    }

    public void setBrokerActive(Boolean brokerActive) {
        this.brokerActive = brokerActive;
    }
}
