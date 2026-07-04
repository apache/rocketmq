/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;

public class ProxyClientAdminEndpointHandler {

    public <T, R> void handle(StreamObserver<R> responseObserver,
        Supplier<ProxyClientAdminResult<T>> action,
        BiFunction<Status, T, R> responseFactory) {
        ProxyClientAdminResult<T> result = this.execute(action);
        R response = this.requireResponseFactory(responseFactory).apply(result.getStatus(), result.getBody());
        ResponseWriter.getInstance().write(responseObserver, response);
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<ProxyClientAdminResult<T>> action) {
        try {
            ProxyClientAdminResult<T> result = this.requireAction(action).get();
            if (result == null) {
                throw new IllegalArgumentException("result is required");
            }
            return result;
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private <T> Supplier<ProxyClientAdminResult<T>> requireAction(Supplier<ProxyClientAdminResult<T>> action) {
        if (action == null) {
            throw new IllegalArgumentException("action is required");
        }
        return action;
    }

    private <T, R> BiFunction<Status, T, R> requireResponseFactory(BiFunction<Status, T, R> responseFactory) {
        if (responseFactory == null) {
            throw new IllegalArgumentException("responseFactory is required");
        }
        return responseFactory;
    }
}
