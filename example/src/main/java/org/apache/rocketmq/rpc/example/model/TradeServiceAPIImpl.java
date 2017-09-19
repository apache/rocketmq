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
import java.util.concurrent.atomic.AtomicLong;

public class TradeServiceAPIImpl implements TradeServiceAPI {

    private final AtomicLong count = new AtomicLong();

    public TradeServiceAPIImpl() {
        super();

    }

    @Override
    public TradeResponse commitOrder(TradeRequest request) throws InterruptedException {
        TradeResponse response = new TradeResponse();
        response.setStoreTimestamp(System.currentTimeMillis());
        return response;
    }

    @Override
    public void deleteOrder(TradeRequest request) throws InterruptedException {
        System.out.println("deleteOrder: " + request);
    }

    @Override
    public void throwUserException(TradeRequest request) throws Exception {
        throw new Exception("User Exception detail message");
    }

    @Override
    public void throwRuntimeException(TradeRequest request) {
        throw new RuntimeException("runtime");
    }

    @Override
    public ArrayList<TradeResponse> getOrderList(final TradeRequest request, final int count) {
        return null;
    }

    @Override
    public int getOrderListSize(final ArrayList<TradeRequest> request, final String obj) {
        return 0;
    }

    @Override
    public String setOrder() {
        return null;
    }
}
