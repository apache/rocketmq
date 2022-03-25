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
package org.apache.rocketmq.proxy.connector;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.rocketmq.client.impl.MQClientAPIExt;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.connector.factory.ForwardClientFactory;

public abstract class AbstractForwardClient implements StartAndShutdown {

    private final ForwardClientFactory clientFactory;
    private MQClientAPIExt[] clients;
    private final String gidPrefix;

    public AbstractForwardClient(ForwardClientFactory clientFactory, String gidPrefix) {
        this.clientFactory = clientFactory;
        this.gidPrefix = gidPrefix;
    }

    protected abstract int getClientNum();

    protected abstract MQClientAPIExt createNewClient(ForwardClientFactory forwardClientFactory, String name);

    protected String getNamePrefix() {
        return this.gidPrefix;
    }

    protected MQClientAPIExt getClient() {
        if (clients.length == 1) {
            return this.clients[0];
        }
        return this.clients[ThreadLocalRandom.current().nextInt(this.clients.length)];
    }

    @Override
    public void start() throws Exception {
        int clientCount = getClientNum();
        this.clients = new MQClientAPIExt[clientCount];

        for (int i = 0; i < clientCount; i++) {
            String name = getNamePrefix() + "N_" + i;
            clients[i] = createNewClient(clientFactory, name);
        }
    }

    @Override
    public void shutdown() throws Exception {

    }
}
