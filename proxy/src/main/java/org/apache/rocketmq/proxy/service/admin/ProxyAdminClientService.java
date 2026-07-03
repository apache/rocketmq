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
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;

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