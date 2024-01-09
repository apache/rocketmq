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
package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;

public class ContextInitPipeline implements RequestPipeline {
    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        Context ctx = Context.current();
        context.withLocalAddress(getDefaultStringMetadataInfo(headers, GrpcConstants.LOCAL_ADDRESS))
            .withRemoteAddress(getDefaultStringMetadataInfo(headers, GrpcConstants.REMOTE_ADDRESS))
            .withClientID(getDefaultStringMetadataInfo(headers, GrpcConstants.CLIENT_ID))
            .withProtocolType(ChannelProtocolType.GRPC_V2.getName())
            .withLanguage(getDefaultStringMetadataInfo(headers, GrpcConstants.LANGUAGE))
            .withClientVersion(getDefaultStringMetadataInfo(headers, GrpcConstants.CLIENT_VERSION))
            .withAction(getDefaultStringMetadataInfo(headers, GrpcConstants.SIMPLE_RPC_NAME));
        if (ctx.getDeadline() != null) {
            context.withRemainingMs(ctx.getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
        }
    }

    protected String getDefaultStringMetadataInfo(Metadata headers, Metadata.Key<String> key) {
        return StringUtils.defaultString(headers.get(key));
    }
}
