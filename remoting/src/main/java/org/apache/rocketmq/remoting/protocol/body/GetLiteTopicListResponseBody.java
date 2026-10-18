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
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class GetLiteTopicListResponseBody extends RemotingSerializable {

    private String parentTopic;

    /**
     * Number of entries included in this response. It may be smaller than the number of lite
     * topics on the broker when {@link #hasMore} is true.
     */
    private int total;
    private boolean hasMore;
    private List<LiteTopicInfo> liteTopicList = new ArrayList<>();

    public String getParentTopic() {
        return parentTopic;
    }

    public void setParentTopic(String parentTopic) {
        this.parentTopic = parentTopic;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

    public List<LiteTopicInfo> getLiteTopicList() {
        return liteTopicList;
    }

    public void setLiteTopicList(List<LiteTopicInfo> liteTopicList) {
        this.liteTopicList = liteTopicList;
    }

    public static class LiteTopicInfo {
        private String liteTopic;
        private long minOffset;
        private long maxOffset;
        private long lastUpdateTimestamp;
        private int subscriberCount;
        private boolean shardingToBroker;

        public String getLiteTopic() {
            return liteTopic;
        }

        public void setLiteTopic(String liteTopic) {
            this.liteTopic = liteTopic;
        }

        public long getMinOffset() {
            return minOffset;
        }

        public void setMinOffset(long minOffset) {
            this.minOffset = minOffset;
        }

        public long getMaxOffset() {
            return maxOffset;
        }

        public void setMaxOffset(long maxOffset) {
            this.maxOffset = maxOffset;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public int getSubscriberCount() {
            return subscriberCount;
        }

        public void setSubscriberCount(int subscriberCount) {
            this.subscriberCount = subscriberCount;
        }

        public boolean isShardingToBroker() {
            return shardingToBroker;
        }

        public void setShardingToBroker(boolean shardingToBroker) {
            this.shardingToBroker = shardingToBroker;
        }
    }
}
