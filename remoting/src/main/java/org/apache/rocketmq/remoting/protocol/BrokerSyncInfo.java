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

package org.apache.rocketmq.remoting.protocol;

public class BrokerSyncInfo extends RemotingSerializable {
    /**
     * For slave online sync, retrieve HA address before register
     */
    private String masterHaAddress;

    private long masterFlushOffset;

    private String masterAddress;

    public BrokerSyncInfo(String masterHaAddress, long masterFlushOffset, String masterAddress) {
        this.masterHaAddress = masterHaAddress;
        this.masterFlushOffset = masterFlushOffset;
        this.masterAddress = masterAddress;
    }

    public String getMasterHaAddress() {
        return masterHaAddress;
    }

    public void setMasterHaAddress(String masterHaAddress) {
        this.masterHaAddress = masterHaAddress;
    }

    public long getMasterFlushOffset() {
        return masterFlushOffset;
    }

    public void setMasterFlushOffset(long masterFlushOffset) {
        this.masterFlushOffset = masterFlushOffset;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    @Override
    public String toString() {
        return "BrokerSyncInfo{" +
            "masterHaAddress='" + masterHaAddress + '\'' +
            ", masterFlushOffset=" + masterFlushOffset +
            ", masterAddress=" + masterAddress +
            '}';
    }
}
