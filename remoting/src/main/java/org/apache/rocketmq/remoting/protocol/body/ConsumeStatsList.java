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
package org.apache.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;

public class ConsumeStatsList extends RemotingSerializable {
    private List<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>> consumeStatsList = new ArrayList<>();
    private String brokerAddr;
    private long totalDiff;
    private long totalInflightDiff;

    public List<Map<String, List<ConsumeStats>>> getConsumeStatsList() {
        return consumeStatsList;
    }

    public void setConsumeStatsList(List<Map<String, List<ConsumeStats>>> consumeStatsList) {
        this.consumeStatsList = consumeStatsList;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public long getTotalDiff() {
        return totalDiff;
    }

    public void setTotalDiff(long totalDiff) {
        this.totalDiff = totalDiff;
    }

    public long getTotalInflightDiff() {
        return totalInflightDiff;
    }

    public void setTotalInflightDiff(long totalInflightDiff) {
        this.totalInflightDiff = totalInflightDiff;
    }
}
