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

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.ActivePublishingSettings;
import apache.rocketmq.v2.ActiveSubscriptionSettings;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.ReportActiveSettingsCommand;
import com.google.protobuf.Duration;
import io.grpc.Context;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;

public class GrpcClientManager {

    public static class ActiveClientSettings {
        private ClientType clientType;
        private Endpoints accessPoint;
        private Duration connectionTimeout;
        private boolean traceOn = true;
        private ActivePublishingSettings activePublishingSettings;
        private ActiveSubscriptionSettings activeSubscriptionSettings;

        public ActiveClientSettings(ReportActiveSettingsCommand reportActiveSettingsCommand) {
            this.clientType = reportActiveSettingsCommand.getClientType();
            this.accessPoint = reportActiveSettingsCommand.getAccessPoint();
            this.connectionTimeout = reportActiveSettingsCommand.getConnectionTimeout();
            this.traceOn = reportActiveSettingsCommand.getTraceOn();
            if (reportActiveSettingsCommand.hasActivePublishingSettings()) {
                this.activePublishingSettings = reportActiveSettingsCommand.getActivePublishingSettings();
            }
            if (reportActiveSettingsCommand.hasActiveSubscriptionSettings()) {
                this.activeSubscriptionSettings = reportActiveSettingsCommand.getActiveSubscriptionSettings();
            }
        }

        public ClientType getClientType() {
            return clientType;
        }

        public Endpoints getAccessPoint() {
            return accessPoint;
        }

        public Duration getConnectionTimeout() {
            return connectionTimeout;
        }

        public boolean isTraceOn() {
            return traceOn;
        }

        public ActivePublishingSettings getActivePublishingSettings() {
            return activePublishingSettings;
        }

        public ActiveSubscriptionSettings getActiveSubscriptionSettings() {
            return activeSubscriptionSettings;
        }
    }

    private static final Map<String, ActiveClientSettings> CLIENT_SETTINGS_MAP = new ConcurrentHashMap<>();

    public ActiveClientSettings getClientSettings(Context ctx) {
        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
        return CLIENT_SETTINGS_MAP.get(clientId);
    }

    public ActiveClientSettings getClientSettings(String clientId) {
        return CLIENT_SETTINGS_MAP.get(clientId);
    }

    public void updateClientSettings(String clientId, ReportActiveSettingsCommand reportActiveSettingsCommand) {
        CLIENT_SETTINGS_MAP.put(clientId, new ActiveClientSettings(reportActiveSettingsCommand));
    }

    public ActiveClientSettings removeClientSettings(String clientId) {
        return CLIENT_SETTINGS_MAP.remove(clientId);
    }
}
