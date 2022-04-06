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

package org.apache.rocketmq.proxy.grpc.service;

import apache.rocketmq.v2.ClientSettings;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;

public class GrpcClientManager {

    private static final Map<String, ClientData> CLIENT_DATA = new ConcurrentHashMap<>();

    public static ClientSettings getClientSettings(Context ctx) {
        return CLIENT_DATA.get(getClientId(ctx)).clientSettings;
    }

    public static void updateClientData(Context ctx, ClientSettings clientSettings, StreamObserver<TelemetryCommand> responseStreamObserver) {
        CLIENT_DATA.put(getClientId(ctx), new ClientData(clientSettings, responseStreamObserver));
    }

    public static String getClientId(Context ctx) {
        return InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
    }

    public static class ClientData {
        private final ClientSettings clientSettings;
        private final StreamObserver<TelemetryCommand> responseStreamObserver;

        public ClientData(ClientSettings clientSettings,
            StreamObserver<TelemetryCommand> responseStreamObserver) {
            this.clientSettings = clientSettings;
            this.responseStreamObserver = responseStreamObserver;
        }
    }
}
