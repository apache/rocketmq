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

package org.apache.rocketmq.grpc.interceptor;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.rocketmq.grpc.common.InterceptorConstants;

public class HeaderServerInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {
        SocketAddress remoteSocketAddress = call.getAttributes()
            .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        String remoteAddress = "";
        if (remoteSocketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteSocketAddress;
            remoteAddress = inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort();
        }
        headers.put(InterceptorConstants.REMOTE_ADDRESS, remoteAddress);

        SocketAddress localSocketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
        String localAddress = "";
        if (localSocketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress)localSocketAddress;
            localAddress = inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort();
        }
        headers.put(InterceptorConstants.LOCAL_ADDRESS, localAddress);

        Context context = Context.current().withValue(InterceptorConstants.METADATA, headers);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
