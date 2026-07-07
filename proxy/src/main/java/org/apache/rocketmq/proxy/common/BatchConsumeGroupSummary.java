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
 * Aggregated summary statistics for batch consumption diagnostics across a consumer group.
 * <p>
 * Provides a high-level view of the batch consumption state for a group,
 * useful for quickly identifying groups with excessive unacked messages,
 * high renewal rates, or expired handles across all clients.
 */
public class BatchConsumeGroupSummary {
    private final String group;
    private final int totalClients;
    private final int totalUnackedMessages;
    private final int totalUnackedHandles;
    private final int totalExpiredHandles;
    private final long totalRenewTimes;
    private final long totalRenewRetryTimes;

    public BatchConsumeGroupSummary(String group, int totalClients,
        int totalUnackedMessages, int totalUnackedHandles, int totalExpiredHandles,
        long totalRenewTimes, long totalRenewRetryTimes) {
        this.group = group;
        this.totalClients = totalClients;
        this.totalUnackedMessages = totalUnackedMessages;
        this.totalUnackedHandles = totalUnackedHandles;
        this.totalExpiredHandles = totalExpiredHandles;
        this.totalRenewTimes = totalRenewTimes;
        this.totalRenewRetryTimes = totalRenewRetryTimes;
    }

    public String getGroup() { return group; }
    public int getTotalClients() { return totalClients; }
    public int getTotalUnackedMessages() { return totalUnackedMessages; }
    public int getTotalUnackedHandles() { return totalUnackedHandles; }
    public int getTotalExpiredHandles() { return totalExpiredHandles; }
    public long getTotalRenewTimes() { return totalRenewTimes; }
    public long getTotalRenewRetryTimes() { return totalRenewRetryTimes; }
}