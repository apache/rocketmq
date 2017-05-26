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

package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

public class QueryConsumeQueueResponseBody extends RemotingSerializable {

    private SubscriptionData subscriptionData;
    private String filterData;
    private List<ConsumeQueueData> queueData;
    private long maxQueueIndex;
    private long minQueueIndex;

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public void setSubscriptionData(SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    public String getFilterData() {
        return filterData;
    }

    public void setFilterData(String filterData) {
        this.filterData = filterData;
    }

    public List<ConsumeQueueData> getQueueData() {
        return queueData;
    }

    public void setQueueData(List<ConsumeQueueData> queueData) {
        this.queueData = queueData;
    }

    public long getMaxQueueIndex() {
        return maxQueueIndex;
    }

    public void setMaxQueueIndex(long maxQueueIndex) {
        this.maxQueueIndex = maxQueueIndex;
    }

    public long getMinQueueIndex() {
        return minQueueIndex;
    }

    public void setMinQueueIndex(long minQueueIndex) {
        this.minQueueIndex = minQueueIndex;
    }
}
