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

package org.apache.rocketmq.proxy.grpc.v1.adapter;

import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;

public class V2Converter {
    public static Resource buildResource(apache.rocketmq.v1.Resource resource) {
        return Resource.newBuilder()
            .setName(resource.getName())
            .setResourceNamespace(resource.getResourceNamespace())
            .build();
    }

    public static HeartbeatRequest buildHeartbeatRequest(apache.rocketmq.v1.HeartbeatRequest request) {
        Resource group;
        if (request.hasProducerData()) {
            group = buildResource(request.getProducerData().getGroup());
        } else if (request.hasConsumerData()) {
            group = buildResource(request.getConsumerData().getGroup());
        } else {
            throw new IllegalArgumentException("HeartbeatRequest is not valid");
        }
        return HeartbeatRequest.newBuilder()
            .setGroup(group)
            .build();
    }

    public static apache.rocketmq.v1.HeartbeatResponse buildHeartbeatResponse(HeartbeatResponse response) {
        return apache.rocketmq.v1.HeartbeatResponse.newBuilder()
            .setCommon(ResponseCommon.newBuilder()
                .setStatus(buildStatus(response.getStatus()))
                .build())
            .build();
    }

    public static com.google.rpc.Status buildStatus(Status status) {
        return com.google.rpc.Status.newBuilder()
            .setCode(buildCodeValue(status.getCode()))
            .setMessage(status.getMessage())
            .build();
    }

    public static int buildCodeValue(Code code) {
        // TODO: complete code mapping
        return com.google.rpc.Code.OK_VALUE;
    }
}
