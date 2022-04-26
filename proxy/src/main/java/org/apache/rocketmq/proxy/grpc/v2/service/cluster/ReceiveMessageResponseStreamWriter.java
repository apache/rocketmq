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
package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;

public abstract class ReceiveMessageResponseStreamWriter {

    protected final StreamObserver<ReceiveMessageResponse> streamObserver;
    protected final ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook;

    public interface Builder {
        ReceiveMessageResponseStreamWriter build(
            StreamObserver<ReceiveMessageResponse> observer,
            ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook);
    }

    public ReceiveMessageResponseStreamWriter(
        StreamObserver<ReceiveMessageResponse> observer,
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook) {
        streamObserver = observer;
        receiveMessageHook = hook;
    }

    public abstract void write(Context ctx, ReceiveMessageRequest request, PopResult result);

    public abstract void write(Context ctx, ReceiveMessageRequest request, Throwable throwable);
}
