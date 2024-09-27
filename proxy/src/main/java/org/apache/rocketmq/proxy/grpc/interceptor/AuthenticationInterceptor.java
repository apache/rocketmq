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

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public class AuthenticationInterceptor implements ServerInterceptor {
    protected final List<AccessValidator> accessValidatorList;

    public AuthenticationInterceptor(List<AccessValidator> accessValidatorList) {
        this.accessValidatorList = accessValidatorList;
    }

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(ServerCall<R, W> call, Metadata headers,
        ServerCallHandler<R, W> next) {
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<R>(next.startCall(call, headers)) {
            @Override
            public void onMessage(R message) {
                GeneratedMessageV3 messageV3 = (GeneratedMessageV3) message;
                headers.put(GrpcConstants.RPC_NAME, messageV3.getDescriptorForType().getFullName());
                headers.put(GrpcConstants.SIMPLE_RPC_NAME, messageV3.getDescriptorForType().getName());
                if (ConfigurationManager.getProxyConfig().isEnableACL()) {
                    try {
                        AuthenticationHeader authenticationHeader = AuthenticationHeader.builder()
                            .remoteAddress(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.REMOTE_ADDRESS))
                            .namespace(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.NAMESPACE_ID))
                            .authorization(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.AUTHORIZATION))
                            .datetime(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.DATE_TIME))
                            .sessionToken(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.SESSION_TOKEN))
                            .requestId(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.REQUEST_ID))
                            .language(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.LANGUAGE))
                            .clientVersion(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.CLIENT_VERSION))
                            .protocol(GrpcConstants.METADATA.get(Context.current()).get(GrpcConstants.PROTOCOL_VERSION))
                            .requestCode(RequestMapping.map(messageV3.getDescriptorForType().getFullName()))
                            .build();

                        validate(authenticationHeader, headers, messageV3);
                        super.onMessage(message);
                    } catch (AclException aclException) {
                        throw new StatusRuntimeException(Status.PERMISSION_DENIED, headers);
                    }
                } else {
                    super.onMessage(message);
                }
            }
        };
    }

    protected void validate(AuthenticationHeader authenticationHeader, Metadata headers, GeneratedMessageV3 messageV3) {
        for (AccessValidator accessValidator : accessValidatorList) {
            AccessResource accessResource = accessValidator.parse(messageV3, authenticationHeader);
            accessValidator.validate(accessResource);

            if (accessResource instanceof PlainAccessResource) {
                PlainAccessResource plainAccessResource = (PlainAccessResource) accessResource;
                headers.put(GrpcConstants.AUTHORIZATION_AK, plainAccessResource.getAccessKey());
            }
        }
    }
}
