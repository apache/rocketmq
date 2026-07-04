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

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;

public class ProxyClientAdminContextFactory {
    private final RequestPipeline requestPipeline;

    public ProxyClientAdminContextFactory(RequestPipeline requestPipeline) {
        if (requestPipeline == null) {
            throw new IllegalArgumentException("requestPipeline is required");
        }
        this.requestPipeline = requestPipeline;
    }

    public ProxyContext create(Metadata headers, GeneratedMessageV3 request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        ProxyContext context = ProxyContext.create();
        this.requestPipeline.execute(context, headers == null ? new Metadata() : headers, request);
        return context;
    }
}
