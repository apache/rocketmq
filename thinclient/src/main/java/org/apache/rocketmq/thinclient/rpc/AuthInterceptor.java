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

package org.apache.rocketmq.thinclient.rpc;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.apache.rocketmq.apis.ClientConfiguration;

public class AuthInterceptor implements ClientInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthInterceptor.class);

    private final ClientConfiguration clientConfiguration;
    private final String clientId;

    public AuthInterceptor(ClientConfiguration clientConfiguration, String clientId) {
        this.clientConfiguration = clientConfiguration;
        this.clientId = clientId;
    }

    private void customMetadata(Metadata headers) {
        try {
            final Metadata metadata = Signature.sign(clientConfiguration, clientId);
            headers.merge(metadata);
        } catch (Throwable t) {
            LOGGER.error("Failed to sign headers, clientId={}", clientId, t);
        }
    }

    @Override
    public <T, E> ClientCall<T, E> interceptCall(MethodDescriptor<T, E> method,
        CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<T, E>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<E> listener, Metadata headers) {
                customMetadata(headers);
                super.start(listener, headers);
            }
        };
    }
}

