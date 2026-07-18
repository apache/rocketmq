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
import io.grpc.Metadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;

public class AuthenticationSubjectPipeline implements RequestPipeline {

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (context == null || headers == null) {
            return;
        }
        String username = StringUtils.trimToNull(headers.get(GrpcConstants.AUTHORIZATION_AK));
        if (username == null) {
            return;
        }
        context.setSubject(User.of(username));
    }
}
