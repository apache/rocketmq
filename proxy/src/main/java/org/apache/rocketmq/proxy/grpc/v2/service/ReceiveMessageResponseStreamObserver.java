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

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseWriter;

public class ReceiveMessageResponseStreamObserver implements StreamObserver<ReceiveMessageResponse> {

    private final Context context;
    private final ReceiveMessageRequest request;
    private final ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook;
    private final StreamObserver<ReceiveMessageResponse> observer;

    public ReceiveMessageResponseStreamObserver(Context context, ReceiveMessageRequest request,
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook,
        StreamObserver<ReceiveMessageResponse> observer) {
        this.context = context;
        this.request = request;
        this.receiveMessageHook = receiveMessageHook;
        this.observer = observer;
    }

    @Override
    public void onNext(ReceiveMessageResponse response) {
        if (receiveMessageHook != null) {
            receiveMessageHook.beforeResponse(context, request, response, null);
        }
        observer.onNext(response);
    }

    @Override
    public void onError(Throwable throwable) {
        if (receiveMessageHook != null) {
            receiveMessageHook.beforeResponse(context, request, null, throwable);
        }
        observer.onError(throwable);
    }

    @Override
    public void onCompleted() {
        observer.onCompleted();
    }

    public boolean isCancelled() {
        return ResponseWriter.isCancelled(observer);
    }
}
