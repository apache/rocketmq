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
package org.apache.rocketmq.remoting.protocol.admin;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumeStats extends RemotingSerializable {
    private Map<MessageQueue, OffsetWrapper> offsetTable = new ConcurrentHashMap<>();
    private double consumeTps = 0;

    public long computeTotalDiff() {
        long diffTotal = 0L;
        for (Entry<MessageQueue, OffsetWrapper> entry : this.offsetTable.entrySet()) {
            diffTotal += entry.getValue().getBrokerOffset() - entry.getValue().getConsumerOffset();
        }
        return diffTotal;
    }

    public long computeInflightTotalDiff() {
        long diffTotal = 0L;
        for (Entry<MessageQueue, OffsetWrapper> entry : this.offsetTable.entrySet()) {
            diffTotal += entry.getValue().getPullOffset() - entry.getValue().getConsumerOffset();
        }
        return diffTotal;
    }

    public Map<MessageQueue, OffsetWrapper> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(Map<MessageQueue, OffsetWrapper> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public double getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(double consumeTps) {
        this.consumeTps = consumeTps;
    }
}
