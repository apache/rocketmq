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

package org.apache.rocketmq.proxy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.common.StartAndShutdown;

public class HealthCheckServer implements StartAndShutdown {

    private HttpServer healthChecker;

    @Override
    public void start() throws Exception {
        this.healthChecker = HttpServer.create(new InetSocketAddress(ConfigurationManager.getProxyConfig().getHealthCheckPort()), 0);
        this.healthChecker.createContext("/status", new HealthCheckHandler());
        this.healthChecker.setExecutor(null);
        this.healthChecker.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        this.healthChecker.stop(0);
        Thread.sleep(TimeUnit.SECONDS.toMillis(ConfigurationManager.getProxyConfig().getWaitAfterStopHealthCheckInSeconds()));
    }

    static class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String response = "Hello";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}