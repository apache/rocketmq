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

package org.apache.rocketmq.store.ha.protocol;

import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class HandshakeSlave {

    private String clusterName;

    private String brokerName;

    private long brokerId;

    private String brokerAddr;

    private int brokerAppVersion;

    private int haProtocolVersion;

    private LanguageCode languageCode;

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

    public int getBrokerAppVersion() {
        return brokerAppVersion;
    }

    public void setBrokerAppVersion(int brokerAppVersion) {
        this.brokerAppVersion = brokerAppVersion;
    }

    public int getHaProtocolVersion() {
        return haProtocolVersion;
    }

    public void setHaProtocolVersion(int haProtocolVersion) {
        this.haProtocolVersion = haProtocolVersion;
    }

    public LanguageCode getLanguageCode() {
        return languageCode;
    }

    public void setLanguageCode(LanguageCode languageCode) {
        this.languageCode = languageCode;
    }

    @Override
    public String toString() {
        return "HandshakeSlave{" +
            "clusterName='" + clusterName + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerId=" + brokerId +
            ", brokerAddr='" + brokerAddr + '\'' +
            ", brokerAppVersion=" + brokerAppVersion +
            ", haProtocolVersion=" + haProtocolVersion +
            ", languageCode=" + languageCode +
            '}';
    }
}
