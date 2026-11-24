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

import java.util.List;
import org.apache.rocketmq.common.lite.LiteLagInfo;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;

public class GetLiteGroupInfoResponseBody extends RemotingSerializable {
    private String group;
    private String parentTopic;
    private String liteTopic;
    // total log info
    private long earliestUnconsumedTimestamp = -1;
    private long totalLagCount;
    // lite topic detail info
    private OffsetWrapper liteTopicOffsetWrapper; // if lite topic specified
    // topK info
    private List<LiteLagInfo> lagCountTopK;
    private List<LiteLagInfo> lagTimestampTopK;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getParentTopic() {
        return parentTopic;
    }

    public void setParentTopic(String parentTopic) {
        this.parentTopic = parentTopic;
    }

    public String getLiteTopic() {
        return liteTopic;
    }

    public void setLiteTopic(String liteTopic) {
        this.liteTopic = liteTopic;
    }

    public long getEarliestUnconsumedTimestamp() {
        return earliestUnconsumedTimestamp;
    }

    public void setEarliestUnconsumedTimestamp(long earliestUnconsumedTimestamp) {
        this.earliestUnconsumedTimestamp = earliestUnconsumedTimestamp;
    }

    public long getTotalLagCount() {
        return totalLagCount;
    }

    public void setTotalLagCount(long totalLagCount) {
        this.totalLagCount = totalLagCount;
    }

    public OffsetWrapper getLiteTopicOffsetWrapper() {
        return liteTopicOffsetWrapper;
    }

    public void setLiteTopicOffsetWrapper(OffsetWrapper liteTopicOffsetWrapper) {
        this.liteTopicOffsetWrapper = liteTopicOffsetWrapper;
    }

    public List<LiteLagInfo> getLagCountTopK() {
        return lagCountTopK;
    }

    public void setLagCountTopK(List<LiteLagInfo> lagCountTopK) {
        this.lagCountTopK = lagCountTopK;
    }

    public List<LiteLagInfo> getLagTimestampTopK() {
        return lagTimestampTopK;
    }

    public void setLagTimestampTopK(List<LiteLagInfo> lagTimestampTopK) {
        this.lagTimestampTopK = lagTimestampTopK;
    }
}
