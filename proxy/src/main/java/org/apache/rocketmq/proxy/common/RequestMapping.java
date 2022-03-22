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

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.ChangeInvisibleDurationRequest;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.SendMessageRequest;
import org.apache.rocketmq.common.protocol.RequestCode;

public class RequestMapping {
    public static int map(String rpcFullName) {
        if (QueryRouteRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.GET_ROUTEINFO_BY_TOPIC;
        }
        if (HeartbeatRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.HEART_BEAT;
        }
        if (HealthCheckRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.HEART_BEAT;
        }
        if (SendMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.SEND_MESSAGE_V2;
        }
        if (QueryAssignmentRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.GET_ROUTEINFO_BY_TOPIC;
        }
        if (ReceiveMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.PULL_MESSAGE;
        }
        if (AckMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.UPDATE_CONSUMER_OFFSET;
        }
        if (NackMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.CONSUMER_SEND_MSG_BACK;
        }
        if (ForwardMessageToDeadLetterQueueResponse.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.CONSUMER_SEND_MSG_BACK;
        }
        if (EndTransactionRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.END_TRANSACTION;
        }
        if (QueryOffsetRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.SEARCH_OFFSET_BY_TIMESTAMP;
        }
        if (PullMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.PULL_MESSAGE;
        }
        if (NotifyClientTerminationRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.UNREGISTER_CLIENT;
        }
        if (ChangeInvisibleDurationRequest.getDescriptor().getFullName().equals(rpcFullName)) {
            return RequestCode.CONSUMER_SEND_MSG_BACK;
        }
        return RequestCode.HEART_BEAT;
    }
}
