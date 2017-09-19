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

package org.apache.rocketmq.rpc;

import java.util.Properties;
import org.apache.rocketmq.remoting.internal.BeanUtils;
import org.apache.rocketmq.rpc.api.SimpleClient;
import org.apache.rocketmq.rpc.api.SimpleServer;
import org.apache.rocketmq.rpc.impl.client.SimpleClientImpl;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;
import org.apache.rocketmq.rpc.impl.server.SimpleServerImpl;

public class RpcBootstrapFactory {
    public static SimpleServer createServerBootstrap(Properties properties) {
        return new SimpleServerImpl(BeanUtils.populate(properties, RpcCommonConfig.class));
    }

    public static SimpleClient createClientBootstrap(Properties properties) {
        return new SimpleClientImpl(BeanUtils.populate(properties, RpcCommonConfig.class));
    }
}
