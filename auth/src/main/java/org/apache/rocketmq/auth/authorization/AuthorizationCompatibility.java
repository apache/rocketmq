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
package org.apache.rocketmq.auth.authorization;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

final class AuthorizationCompatibility {

    private AuthorizationCompatibility() {
    }

    static boolean matches(RemotingCommand request) {
        if (request == null) {
            return false;
        }
        try {
            switch (request.getCode()) {
                case RequestCode.HEART_BEAT:
                    return isProducerHeartbeat(request);
                case RequestCode.UNREGISTER_CLIENT:
                    return isProducerUnregister(request);
                case RequestCode.END_TRANSACTION:
                case RequestCode.VIEW_MESSAGE_BY_ID:
                    return isHistoricalTopicAbsent(request);
                default:
                    return false;
            }
        } catch (Throwable ignored) {
            return false;
        }
    }

    static boolean matches(GeneratedMessageV3 request) {
        if (request instanceof HeartbeatRequest) {
            HeartbeatRequest heartbeat = (HeartbeatRequest) request;
            return StringUtils.isBlank(heartbeat.getGroup().getName())
                && (heartbeat.getClientType() == ClientType.PRODUCER
                || heartbeat.getClientType() == ClientType.CLIENT_TYPE_UNSPECIFIED);
        }
        if (request instanceof NotifyClientTerminationRequest) {
            NotifyClientTerminationRequest termination = (NotifyClientTerminationRequest) request;
            return StringUtils.isBlank(termination.getGroup().getName());
        }
        if (request instanceof TelemetryCommand) {
            TelemetryCommand telemetry = (TelemetryCommand) request;
            switch (telemetry.getCommandCase()) {
                case SETTINGS:
                    return telemetry.getSettings().hasPublishing()
                        && telemetry.getSettings().getPublishing().getTopicsCount() == 0;
                case THREAD_STACK_TRACE:
                case VERIFY_MESSAGE_RESULT:
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    private static boolean isProducerHeartbeat(RemotingCommand request) {
        if (request.getBody() == null) {
            return false;
        }
        HeartbeatData heartbeat = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        if (heartbeat == null || CollectionUtils.isNotEmpty(heartbeat.getConsumerDataSet())
            || CollectionUtils.isEmpty(heartbeat.getProducerDataSet())) {
            return false;
        }
        for (ProducerData producer : heartbeat.getProducerDataSet()) {
            if (producer == null || producer.getGroupName() == null) {
                return false;
            }
        }
        return true;
    }

    private static boolean isProducerUnregister(RemotingCommand request) {
        return StringUtils.isNotBlank(getExtField(request, "producerGroup"))
            && StringUtils.isBlank(getExtField(request, "consumerGroup"));
    }

    /**
     * Historical END_TRANSACTION and VIEW_MESSAGE_BY_ID requests carry no topic field.
     */
    private static boolean isHistoricalTopicAbsent(RemotingCommand request) {
        return request.getExtFields() != null && StringUtils.isBlank(getExtField(request, "topic"));
    }

    private static String getExtField(RemotingCommand request, String name) {
        return request.getExtFields() == null ? null : request.getExtFields().get(name);
    }
}
