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
package org.apache.rocketmq.rpc.example.quickstart;

import java.util.Properties;
import org.apache.rocketmq.rpc.RpcBootstrapFactory;
import org.apache.rocketmq.rpc.api.SimpleClient;
import org.apache.rocketmq.rpc.example.model.TradeRequest;
import org.apache.rocketmq.rpc.example.model.TradeResponse;
import org.apache.rocketmq.rpc.example.model.TradeServiceAPI;

public class Client {
    public static void main(String[] args) throws InterruptedException {
        SimpleClient client = RpcBootstrapFactory.createClientBootstrap(new Properties());
        TradeServiceAPI tradeService = client.bind(TradeServiceAPI.class, "127.0.0.1:8888", new Properties());
        client.start();

        TradeResponse tradeResponse = tradeService.commitOrder(new TradeRequest());
        System.out.println(tradeResponse);
    }
}
