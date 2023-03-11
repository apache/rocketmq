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

package org.apache.rocketmq.store.ha.autoswitch;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class BrokerMetadata extends MetadataFile {

    protected String clusterName;

    protected String brokerName;

    protected Long brokerId;

    public BrokerMetadata(String filePath) {
        this.filePath = filePath;
    }

    public void updateAndPersist(String clusterName, String brokerName, Long brokerId) throws Exception {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        writeToFile();
    }

    @Override
    public String encodeToStr() {
        StringBuilder sb = new StringBuilder();
        sb.append(clusterName).append("#");
        sb.append(brokerName).append("#");
        sb.append(brokerId);
        return sb.toString();
    }

    @Override
    public void decodeFromStr(String dataStr) {
        if (dataStr == null) return;
        String[] dataArr = dataStr.split("#");
        this.clusterName = dataArr[0];
        this.brokerName = dataArr[1];
        this.brokerId = Long.valueOf(dataArr[2]);
    }

    @Override
    public boolean isLoaded() {
        return StringUtils.isNotEmpty(this.clusterName) && StringUtils.isNotEmpty(this.brokerName) && brokerId != null;
    }

    @Override
    public void clearInMem() {
        this.clusterName = null;
        this.brokerName = null;
        this.brokerId = null;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerMetadata that = (BrokerMetadata) o;
        return Objects.equals(clusterName, that.clusterName) && Objects.equals(brokerName, that.brokerName) && Objects.equals(brokerId, that.brokerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, brokerName, brokerId);
    }

    @Override
    public String toString() {
        return "BrokerMetadata{" +
                "clusterName='" + clusterName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerId=" + brokerId +
                ", filePath='" + filePath + '\'' +
                '}';
    }
}
