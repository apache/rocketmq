/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.common.admin;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


/**
 *
 * @author shijia.wxr
 *
 */
public class ConsumeStats extends RemotingSerializable {
    private HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<MessageQueue, OffsetWrapper>();
    private double consumeTps = 0;


    public long computeTotalDiff() {
        long diffTotal = 0L;

        Iterator<Entry<MessageQueue, OffsetWrapper>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, OffsetWrapper> next = it.next();
            long diff = next.getValue().getBrokerOffset() - next.getValue().getConsumerOffset();
            diffTotal += diff;
        }

        return diffTotal;
    }


    public HashMap<MessageQueue, OffsetWrapper> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(HashMap<MessageQueue, OffsetWrapper> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public double getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(double consumeTps) {
        this.consumeTps = consumeTps;
    }
}
