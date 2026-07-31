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

package org.apache.rocketmq.proxy.service.admin;

import java.util.List;
import org.apache.rocketmq.proxy.common.BatchConsumeClientDiagnostics;
import org.apache.rocketmq.proxy.common.BatchConsumeGroupSummary;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager.PopReceiptHandleDiagnosticResult;

/**
 * Proxy Admin Client Service interface.
 * Provides online client query capabilities for the Proxy Admin interface.
 * <p>
 * This is the core service layer for RIP-2 M1 (Online Client Query).
 * All data comes from the internal ClientManager module.
 */
public interface ProxyAdminClientService {

    /**
     * List online gRPC clients with filtering and pagination.
     * Supports filter pushdown to avoid full memory traversal.
     *
     * @param filter  filter criteria including group, topic, clientIdPrefix, language, time range
     * @param pageNum page number starting from 1
     * @param pageSize page size, max 100
     * @return paginated list of client instances
     */
    ListClientsResult listClients(ListClientsFilter filter, int pageNum, int pageSize);

    /**
     * Describe a single client's detailed information.
     * Returns the complete Telemetry view and diagnostic info.
     *
     * @param clientId the unique client identifier
     * @return detailed client information
     */
    ClientDetailInfo describeClient(String clientId);

    /**
     * List online clients by consumer group.
     * High-frequency shortcut interface for operations.
     *
     * @param group    consumer group name
     * @param pageNum  page number starting from 1
     * @param pageSize page size, max 100
     * @return paginated list of client instances in the group
     */
    ListClientsResult listClientsByGroup(String group, int pageNum, int pageSize);

    /**
     * List online clients by topic.
     * High-frequency shortcut interface for operations.
     *
     * @param topic    topic name
     * @param pageNum  page number starting from 1
     * @param pageSize page size, max 100
     * @return paginated list of client instances associated with the topic
     */
    ListClientsResult listClientsByTopic(String topic, int pageNum, int pageSize);

    /**
     * Record a heartbeat event for a client.
     * Called by the telemetry processing pipeline when a heartbeat is received.
     * This enables real heartbeat history tracking (RIP-2 §5.2.2).
     *
     * @param clientId the client identifier
     */
    void recordHeartbeat(String clientId);

    /**
     * Force disconnect a specific client connection.
     * Closes the gRPC telemetry stream, removes the channel and settings,
     * triggering client reconnection and consumer group rebalance.
     * <p>
     * Use cases:
     * - Malicious client detection and isolation
     * - Stuck consumer triggering rebalance
     * - Zombie connection cleanup
     *
     * @param clientId the unique client identifier to disconnect
     * @param reason   human-readable reason for audit logging
     * @return true if the client was found and disconnected, false if not found
     */
    boolean forceDisconnectClient(String clientId, String reason);

    /**
     * Query POP receipt handles for diagnostics.
     * <p>
     * Provides diagnostic information for POP consumption mode, including:
     * - Unacked message receipt handles with renewal statistics
     * - Messages with expired invisible time (about to be redelivered)
     * - Frequent ChangeInvisibleTime (renewal) patterns
     * - Consumption timeout detection
     * <p>
     * This is the core service method for RIP-2 M3 (POP Diagnostics).
     *
     * @param group    consumer group name (required)
     * @param topic    optional topic filter, null or empty means no filter
     * @param pageNum  page number starting from 1
     * @param pageSize page size, max 100
     * @return diagnostic result containing summary and paginated handle details
     */
    PopReceiptHandleDiagnosticResult describePopReceiptHandles(String group, String topic, int pageNum, int pageSize);

    /**
     * Query batch consumption diagnostics, aggregated per client.
     * <p>
     * Provides diagnostic information for batch consumption mode, including:
     * - Per-client unacked message counts and handle counts
     * - Clients with expired handles (messages about to be redelivered)
     * - Renewal patterns per client (ChangeInvisibleTime frequency)
     * - Topic distribution of unacked messages per client
     * - Client configuration correlation (receiveBatchSize, longPollingTimeout)
     * <p>
     * This is the core service method for RIP-2 M4 (Batch Consume Diagnostics).
     *
     * @param group    consumer group name (required)
     * @param topic    optional topic filter, null or empty means no filter
     * @param clientId optional client ID filter for exact match
     * @param pageNum  page number starting from 1
     * @param pageSize page size, max 100
     * @return diagnostic result containing summary and paginated per-client diagnostics
     */
    BatchConsumeDiagnosticResult describeBatchConsumeDiagnostics(String group, String topic, String clientId, int pageNum, int pageSize);

    /**
     * Result of batch consumption diagnostic query.
     */
    class BatchConsumeDiagnosticResult {
        private final BatchConsumeGroupSummary summary;
        private final List<BatchConsumeClientDiagnostics> diagnostics;
        private final long total;
        private final int pageNum;
        private final int pageSize;

        public BatchConsumeDiagnosticResult(BatchConsumeGroupSummary summary,
            List<BatchConsumeClientDiagnostics> diagnostics, long total, int pageNum, int pageSize) {
            this.summary = summary;
            this.diagnostics = diagnostics;
            this.total = total;
            this.pageNum = pageNum;
            this.pageSize = pageSize;
        }

        public BatchConsumeGroupSummary getSummary() { return summary; }
        public List<BatchConsumeClientDiagnostics> getDiagnostics() { return diagnostics; }
        public long getTotal() { return total; }
        public int getPageNum() { return pageNum; }
        public int getPageSize() { return pageSize; }
    }

    /**
     * Result of list clients query with pagination info.
     */
    class ListClientsResult {
        private final long total;
        private final int pageNum;
        private final int pageSize;
        private final List<ClientInstanceInfo> list;

        public ListClientsResult(long total, int pageNum, int pageSize, List<ClientInstanceInfo> list) {
            this.total = total;
            this.pageNum = pageNum;
            this.pageSize = pageSize;
            this.list = list;
        }

        public long getTotal() {
            return total;
        }

        public int getPageNum() {
            return pageNum;
        }

        public int getPageSize() {
            return pageSize;
        }

        public List<ClientInstanceInfo> getList() {
            return list;
        }
    }
}