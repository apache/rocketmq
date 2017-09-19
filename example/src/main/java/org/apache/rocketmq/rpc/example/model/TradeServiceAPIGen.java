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

package org.apache.rocketmq.rpc.example.model;

import java.util.ArrayList;
import org.apache.rocketmq.rpc.annotation.MethodType;
import org.apache.rocketmq.rpc.annotation.RemoteMethod;
import org.apache.rocketmq.rpc.annotation.RemoteService;
import org.apache.rocketmq.rpc.api.Promise;

@RemoteService(name = "TradeServiceAPI")
public interface TradeServiceAPIGen extends TradeServiceAPI {
    @RemoteMethod(name = "commitOrder", type = MethodType.ASYNC)
    Promise<TradeResponse> commitOrderAsync(TradeRequest request);

    @RemoteMethod(name = "commitOrder", type = MethodType.ONEWAY)
    void commitOrderOneway(final TradeRequest request);

    @RemoteMethod(name = "deleteOrder", type = MethodType.ASYNC)
    Promise<TradeResponse> deleteOrderAsync(TradeRequest request);

    @RemoteMethod(name = "deleteOrder", type = MethodType.ONEWAY)
    void deleteOrderOneway(TradeRequest request);

    @RemoteMethod(name = "throwUserException", type = MethodType.ASYNC)
    void throwUserExceptionAsync(TradeRequest request);

    @RemoteMethod(name = "throwUserException", type = MethodType.ONEWAY)
    void throwUserExceptionOneway(TradeRequest request);

    @RemoteMethod(name = "throwRuntimeException", type = MethodType.ASYNC)
    void throwRuntimeExceptionAsync(TradeRequest request);

    @RemoteMethod(name = "throwRuntimeException", type = MethodType.ONEWAY)
    void throwRuntimeExceptionOneway(TradeRequest request);

    @RemoteMethod(name = "getOrderList", type = MethodType.ASYNC)
    Promise<ArrayList<TradeResponse>> getOrderListAsync(TradeRequest request, int count);

    @RemoteMethod(name = "getOrderListSize", type = MethodType.ASYNC)
    Promise<Integer> getOrderListSizeAsync(ArrayList<TradeRequest> request, String obj);

    @RemoteMethod(name = "setOrder", type = MethodType.ASYNC)
    Promise<String> setOrderAsync();
}
