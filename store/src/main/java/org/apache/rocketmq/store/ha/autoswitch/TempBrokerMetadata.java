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

public class TempBrokerMetadata extends MetadataFile {

    private String clusterName;

    private String brokerName;

    private Long brokerId;

    private String registerCheckCode;

    public TempBrokerMetadata(String filePath) {
        this(filePath, null, null, null, null);
    }

    public TempBrokerMetadata(String filePath, String clusterName, String brokerName, Long brokerId, String registerCheckCode) {
        this.filePath = filePath;
        this.clusterName = clusterName;
        this.brokerId = brokerId;
        this.brokerName = brokerName;
        this.registerCheckCode = registerCheckCode;
    }

    public void updateAndPersist(String clusterName, String brokerName, Long brokerId, String registerCheckCode) throws Exception {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.registerCheckCode = registerCheckCode;
        writeToFile();
    }

    @Override
    public String encodeToStr() {
        StringBuilder sb = new StringBuilder();
        sb.append(clusterName).append("#");
        sb.append(brokerName).append("#");
        sb.append(brokerId).append("#");
        sb.append(registerCheckCode);
        return sb.toString();
    }

    @Override
    public void decodeFromStr(String dataStr) {
        if (dataStr == null) return;
        String[] dataArr = dataStr.split("#");
        this.clusterName = dataArr[0];
        this.brokerName = dataArr[1];
        this.brokerId = Long.valueOf(dataArr[2]);
        this.registerCheckCode = dataArr[3];
    }

    @Override
    public boolean isLoaded() {
        return StringUtils.isNotEmpty(this.clusterName) && StringUtils.isNotEmpty(this.brokerName) && brokerId != null && StringUtils.isNotEmpty(this.registerCheckCode);
    }

    @Override
    public void clearInMem() {
        this.clusterName = null;
        this.brokerName = null;
        this.brokerId = null;
        this.registerCheckCode = null;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public String getRegisterCheckCode() {
        return registerCheckCode;
    }
}
