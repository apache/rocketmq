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

package org.apache.rocketmq.proxy.grpc.interceptor;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.RequestCode;

public class RequestMapping {
    @SuppressWarnings("DoubleBraceInitialization")
    private final static Map<String, Integer> REQUEST_MAP = new HashMap<String, Integer>() {
        {
            // v2
            put(QueryRouteRequest.getDescriptor().getFullName(), RequestCode.GET_ROUTEINFO_BY_TOPIC);
            put(HeartbeatRequest.getDescriptor().getFullName(), RequestCode.HEART_BEAT);
            put(SendMessageRequest.getDescriptor().getFullName(), RequestCode.SEND_MESSAGE_V2);
            put(RecallMessageRequest.getDescriptor().getFullName(), RequestCode.RECALL_MESSAGE);
            put(QueryAssignmentRequest.getDescriptor().getFullName(), RequestCode.GET_ROUTEINFO_BY_TOPIC);
            put(ReceiveMessageRequest.getDescriptor().getFullName(), RequestCode.PULL_MESSAGE);
            put(AckMessageRequest.getDescriptor().getFullName(), RequestCode.UPDATE_CONSUMER_OFFSET);
            put(ForwardMessageToDeadLetterQueueResponse.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
            put(EndTransactionRequest.getDescriptor().getFullName(), RequestCode.END_TRANSACTION);
            put(NotifyClientTerminationRequest.getDescriptor().getFullName(), RequestCode.UNREGISTER_CLIENT);
            put(ChangeInvisibleDurationRequest.getDescriptor().getFullName(), RequestCode.CONSUMER_SEND_MSG_BACK);
        }
    };

    public static int map(String rpcFullName) {
        if (REQUEST_MAP.containsKey(rpcFullName)) {
            return REQUEST_MAP.get(rpcFullName);
        }
        return RequestCode.HEART_BEAT;
    }
}
