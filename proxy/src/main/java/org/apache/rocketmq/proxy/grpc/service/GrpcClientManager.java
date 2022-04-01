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

import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;

public class GrpcClientManager {

    private static final Map<String, ClientData> CLIENT_DATAS = new ConcurrentHashMap<>();

    public static Settings getClientSettings(Context ctx) {
        return CLIENT_DATAS.get(getClientId(ctx)).settings;
    }

    public static void updateClientData(Context ctx, Settings settings, StreamObserver<TelemetryCommand> responseStreamObserver) {
        CLIENT_DATAS.put(getClientId(ctx), new ClientData(settings, responseStreamObserver));
    }

    public static String getClientId(Context ctx) {
        return InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
    }

    public static class ClientData {
        private final Settings settings;
        private final StreamObserver<TelemetryCommand> responseStreamObserver;

        public ClientData(Settings settings,
            StreamObserver<TelemetryCommand> responseStreamObserver) {
            this.settings = settings;
            this.responseStreamObserver = responseStreamObserver;
        }
    }
}
