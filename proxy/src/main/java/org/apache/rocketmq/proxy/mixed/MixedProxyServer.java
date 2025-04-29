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
package org.apache.rocketmq.proxy.mixed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.spi.ProxyServer;
import org.apache.rocketmq.proxy.spi.ProxyServerBase;

public class MixedProxyServer extends ProxyServerBase {

    private List<ProxyServer> proxyServers;

    @Override
    public void shutdown() throws Exception {
        for (ProxyServer proxyServer : proxyServers) {
            proxyServer.shutdown();
        }
    }

    @Override
    public void start() throws Exception {
        for (ProxyServer proxyServer : this.proxyServers) {
            proxyServer.start();
        }
    }

    @Override
    public void preShutdown() throws Exception {
        for (ProxyServer proxyServer : this.proxyServers) {
            proxyServer.preShutdown();
        }
    }

    @Override
    public List<StartAndShutdown> getStartAndShutdowns() {
        Set<StartAndShutdown> startAndShutdowns = new HashSet<>();
        for (ProxyServer proxyServer : this.proxyServers) {
            startAndShutdowns.addAll(proxyServer.getStartAndShutdowns());
        }
        return new ArrayList<>(startAndShutdowns);
    }

    public void setProxyServers(List<ProxyServer> proxyServers) {
        this.proxyServers = proxyServers;
    }

    public List<ProxyServer> getProxyServers() {
        return proxyServers;
    }

}
