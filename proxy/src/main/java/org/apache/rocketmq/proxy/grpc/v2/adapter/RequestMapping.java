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

package org.apache.rocketmq.proxy.grpc.v2.adapter;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.protocol.RequestCode;

public class RequestMapping {
    private final static Map<String, Integer> REQUEST_MAP = new HashMap<String, Integer>() {
        {
            // v2
            put(QueryRouteRequest.getDescriptor().getFullName(), RequestCode.GET_ROUTEINFO_BY_TOPIC);
            put(HeartbeatRequest.getDescriptor().getFullName(), RequestCode.HEART_BEAT);
            put(SendMessageRequest.getDescriptor().getFullName(), RequestCode.SEND_MESSAGE_V2);
            put(QueryAssignmentRequest.getDescriptor().getFullName(), RequestCode.GET_ROUTEINFO_BY_TOPIC);
            put(ReceiveMessageRequest.getDescriptor().getFullName(), RequestCode.PULL_MESSAGE);
            put(AckMessageRequest.getDescriptor().getFullName(), RequestCode.UPDATE_CONSUMER_OFFSET);
            put(NackMessageRequest.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
            put(ForwardMessageToDeadLetterQueueResponse.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
            put(EndTransactionRequest.getDescriptor().getFullName(), RequestCode.END_TRANSACTION);
            put(NotifyClientTerminationRequest.getDescriptor().getFullName(), RequestCode.UNREGISTER_CLIENT);
            put(ChangeInvisibleDurationRequest.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);

            // v1
            put(apache.rocketmq.v1.QueryRouteRequest.getDescriptor().getFullName(), RequestCode.GET_ROUTEINFO_BY_TOPIC);
            put(apache.rocketmq.v1.HeartbeatRequest.getDescriptor().getFullName(), RequestCode.HEART_BEAT);
            put(apache.rocketmq.v1.HealthCheckRequest.getDescriptor().getFullName(), RequestCode.HEART_BEAT);
            put(apache.rocketmq.v1.SendMessageRequest.getDescriptor().getFullName(), RequestCode.SEND_MESSAGE_V2);
            put(apache.rocketmq.v1.QueryAssignmentRequest.getDescriptor().getFullName(), RequestCode.GET_ROUTEINFO_BY_TOPIC);
            put(apache.rocketmq.v1.ReceiveMessageRequest.getDescriptor().getFullName(), RequestCode.PULL_MESSAGE);
            put(apache.rocketmq.v1.AckMessageRequest.getDescriptor().getFullName(), RequestCode.UPDATE_CONSUMER_OFFSET);
            put(apache.rocketmq.v1.NackMessageRequest.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
            put(apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
            put(apache.rocketmq.v1.EndTransactionRequest.getDescriptor().getFullName(), RequestCode.END_TRANSACTION);
            put(apache.rocketmq.v1.QueryOffsetRequest.getDescriptor().getFullName(), RequestCode.SEARCH_OFFSET_BY_TIMESTAMP);
            put(apache.rocketmq.v1.PullMessageRequest.getDescriptor().getFullName(), RequestCode.PULL_MESSAGE);
            put(apache.rocketmq.v1.NotifyClientTerminationRequest.getDescriptor().getFullName(), RequestCode.UNREGISTER_CLIENT);
            put(apache.rocketmq.v1.ChangeInvisibleDurationRequest.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
        }
    };

    public static int map(String rpcFullName) {
        if (REQUEST_MAP.containsKey(rpcFullName)) {
            return REQUEST_MAP.get(rpcFullName);
        }
        return RequestCode.HEART_BEAT;
    }
}
