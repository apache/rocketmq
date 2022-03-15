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
package org.apache.rocketmq.proxy.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIExtImpl;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.client.factory.ForwardClientFactory;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class DefaultForwardClient extends AbstractForwardClient {
    private static final String CID_PREFIX = "CID_RMQ_PROXY_DEFAULT_";

    public DefaultForwardClient(ForwardClientFactory clientFactory) {
        super(clientFactory);
    }

    @Override
    protected int getClientNum() {
        return ConfigurationManager.getProxyConfig().getDefaultForwardClientNum();
    }

    @Override
    protected MQClientAPIExtImpl createNewClient(ForwardClientFactory clientFactory, String name) {
        double workerFactor = ConfigurationManager.getProxyConfig().getDefaultForwardClientWorkerFactor();
        final int threadCount = (int) Math.ceil(Runtime.getRuntime().availableProcessors() * workerFactor);

        return clientFactory.getMQClient(name, threadCount);
    }

    @Override
    protected String getNamePrefix() {
        return CID_PREFIX;
    }

    public CompletableFuture<List<String>> getConsumerListByGroup(
        String brokerAddr, GetConsumerListByGroupRequestHeader requestHeader, long timeoutMillis) {
        return getClient().getConsumerListByGroup(brokerAddr, requestHeader, timeoutMillis);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        return getClient().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
    }
}
