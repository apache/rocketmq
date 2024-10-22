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

import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.proxy.spi.ProxyServer;
import org.apache.rocketmq.proxy.spi.ProxyServerBase;
import org.apache.rocketmq.proxy.spi.ProxyServerFactory;
import org.apache.rocketmq.proxy.spi.ProxyServerFactoryBase;

import java.util.ArrayList;
import java.util.List;

public class MixedProxyServerBuilder extends ProxyServerFactoryBase {

    @Override
    protected ProxyServerBase build() {
        String prefix = "META-INF/services/" + ProxyServerFactory.class.getName();
        List<ProxyServerFactory> factories = ServiceProvider.load(prefix, ProxyServerFactory.class);
        List<ProxyServer> proxyServers = new ArrayList<ProxyServer>();
        for (ProxyServerFactory psf : factories) {
            if (psf instanceof MixedProxyServerBuilder) {
                continue;
            }
            proxyServers.add(psf.withAccessValidators(validators).withInitializer(initializer).get());
        }
        MixedProxyServer server = new MixedProxyServer();
        server.setProxyServers(proxyServers);
        return server;
    }

}
