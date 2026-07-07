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

package org.apache.rocketmq.proxy.common;

/**
 * Aggregated summary statistics for POP receipt handles in a consumer group.
 * <p>
 * Provides a high-level view of the POP consumption state for a group,
 * useful for quickly identifying groups with excessive unacked messages,
 * high renewal rates, or expired handles.
 */
public class PopReceiptHandleGroupSummary {
    private final String group;
    private final int totalHandles;
    private final int totalMessages;
    private final long totalRenewTimes;
    private final long totalRenewRetryTimes;
    private final int expiredHandles;

    public PopReceiptHandleGroupSummary(String group, int totalHandles, int totalMessages,
        long totalRenewTimes, long totalRenewRetryTimes, int expiredHandles) {
        this.group = group;
        this.totalHandles = totalHandles;
        this.totalMessages = totalMessages;
        this.totalRenewTimes = totalRenewTimes;
        this.totalRenewRetryTimes = totalRenewRetryTimes;
        this.expiredHandles = expiredHandles;
    }

    public String getGroup() { return group; }
    public int getTotalHandles() { return totalHandles; }
    public int getTotalMessages() { return totalMessages; }
    public long getTotalRenewTimes() { return totalRenewTimes; }
    public long getTotalRenewRetryTimes() { return totalRenewRetryTimes; }
    public int getExpiredHandles() { return expiredHandles; }
}