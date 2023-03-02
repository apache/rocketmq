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

package org.apache.rocketmq.proxy.grpc;

import java.util.concurrent.TimeUnit;
import io.grpc.Server;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.StartAndShutdown;

public class GrpcServer implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final Server server;

    protected GrpcServer(Server server) {
        this.server = server;
    }

    public void start() throws Exception {
        this.server.start();
        log.info("grpc server start successfully.");
    }

    public void shutdown() {
        try {
            this.server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            log.info("grpc server shutdown successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}