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

package org.apache.rocketmq.remoting.impl.netty;

import java.util.Properties;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.apache.rocketmq.remoting.internal.BeanUtils;
import org.apache.rocketmq.remoting.internal.PropertyUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Remoting Bootstrap entrance.
 */
public final class RemotingBootstrapFactory {
    public static RemotingClient createRemotingClient(@NotNull final String fileName) {
        Properties prop = PropertyUtils.loadProps(fileName);
        RemotingConfig config = BeanUtils.populate(prop, RemotingConfig.class);
        return new NettyRemotingClient(config);
    }

    public static RemotingClient createRemotingClient(@NotNull final RemotingConfig config) {
        return new NettyRemotingClient(config);
    }

    public static RemotingClient createRemotingClient(@NotNull final Properties properties) {
        RemotingConfig config = BeanUtils.populate(properties, RemotingConfig.class);
        return new NettyRemotingClient(config);
    }

    public static NettyRemotingServer createRemotingServer(@NotNull final String fileName) {
        Properties prop = PropertyUtils.loadProps(fileName);
        RemotingConfig config = BeanUtils.populate(prop, RemotingConfig.class);
        return new NettyRemotingServer(config);
    }

    public static NettyRemotingServer createRemotingServer(@NotNull final Properties properties) {
        RemotingConfig config = BeanUtils.populate(properties, RemotingConfig.class);
        return new NettyRemotingServer(config);
    }

    public static NettyRemotingServer createRemotingServer(@NotNull final RemotingConfig config) {
        return new NettyRemotingServer(config);
    }
}
