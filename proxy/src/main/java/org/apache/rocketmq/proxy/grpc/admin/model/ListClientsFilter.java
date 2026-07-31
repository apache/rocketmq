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

package org.apache.rocketmq.proxy.grpc.admin.model;

/**
 * Filter criteria for ListClients query.
 * Supports filter pushdown to avoid full memory traversal.
 */
public class ListClientsFilter {
    private String group;
    private String topic;
    private String clientIdPrefix;
    private String language;
    private long connectTimeStart;
    private long connectTimeEnd;

    public ListClientsFilter() {
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getClientIdPrefix() {
        return clientIdPrefix;
    }

    public void setClientIdPrefix(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public long getConnectTimeStart() {
        return connectTimeStart;
    }

    public void setConnectTimeStart(long connectTimeStart) {
        this.connectTimeStart = connectTimeStart;
    }

    public long getConnectTimeEnd() {
        return connectTimeEnd;
    }

    public void setConnectTimeEnd(long connectTimeEnd) {
        this.connectTimeEnd = connectTimeEnd;
    }

    /**
     * Check if any filter criteria is set.
     *
     * @return true if at least one filter criterion is set
     */
    public boolean hasFilter() {
        return group != null || topic != null || clientIdPrefix != null
            || language != null || connectTimeStart > 0 || connectTimeEnd > 0;
    }
}