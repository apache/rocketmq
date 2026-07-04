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

package org.apache.rocketmq.proxy.grpc.v2;

import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationSubjectPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthorizationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public final class GrpcRequestPipelineFactory {

    private GrpcRequestPipelineFactory() {
    }

    public static RequestPipeline create(MessagingProcessor messagingProcessor) {
        RequestPipeline pipeline = (context, headers, request) -> {
        };
        AuthConfig authConfig = ConfigurationManager.getAuthConfig();
        if (authConfig != null) {
            pipeline = pipeline
                .pipe(new AuthorizationPipeline(authConfig, messagingProcessor))
                .pipe(new AuthenticationSubjectPipeline())
                .pipe(new AuthenticationPipeline(authConfig, messagingProcessor));
        }
        return pipeline.pipe(new ContextInitPipeline());
    }
}
