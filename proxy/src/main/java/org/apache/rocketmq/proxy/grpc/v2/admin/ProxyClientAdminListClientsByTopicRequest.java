/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;

public class ProxyClientAdminListClientsByTopicRequest extends ProxyClientAdminListClientsRequest {
    private final String topic;

    private ProxyClientAdminListClientsByTopicRequest(Builder builder) {
        super(builder);
        this.topic = normalizeTopic(builder.topic);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public ProxyClientQuery toQuery() {
        return this.populateQueryBuilder(ProxyClientQuery.newBuilder())
            .setTopic(topic)
            .build();
    }

    private static String normalizeTopic(String topic) {
        String normalizedTopic = StringUtils.trimToNull(topic);
        if (normalizedTopic == null) {
            return null;
        }
        if (normalizedTopic.length() > Validators.TOPIC_MAX_LENGTH) {
            throw new IllegalArgumentException("topic length exceeds topic max length "
                + Validators.TOPIC_MAX_LENGTH);
        }
        return normalizedTopic;
    }

    public static class Builder extends ProxyClientAdminListClientsRequest.Builder<Builder> {
        private String topic;

        public Builder setTopic(String topic) {
            this.topic = StringUtils.trimToNull(topic);
            return this;
        }

        @Override
        public ProxyClientAdminListClientsByTopicRequest build() {
            return new ProxyClientAdminListClientsByTopicRequest(this);
        }
    }
}
